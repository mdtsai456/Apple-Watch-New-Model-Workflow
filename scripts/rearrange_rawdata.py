from pathlib import Path

import pandas as pd


# Server
INPUT_ROOT = Path("/app/data/rawdata")
OUTPUT_ROOT = Path("/app/data/rawdata_rearranged")
# Local
# INPUT_ROOT = Path("/data/rawdata")
# OUTPUT_ROOT = Path("/data/rawdata_rearranged")
# 每個輸出檔案的最大列數
CHUNK_SIZE = 3000


def parse_model_name(csv_path: Path) -> str | None:
    """從 CSV 檔名中擷取型號名稱。

    e.g. "sensor_SE2_20240101.csv" → "SE2"
    回傳 None 表示檔名格式不符預期，呼叫端應跳過該檔案。
    """
    # 檔名格式預期為 <prefix>_<model>_...，取第二段作為型號名稱
    parts = csv_path.stem.split("_")
    if len(parts) < 2:
        print(f"  WARNING: skipping file with unexpected name format: {csv_path.name}")
        return None
    return parts[1]


def find_next_output_index(model_dir: Path, model: str) -> int:
    """掃描已存在的輸出檔，回傳下一個可用的編號，避免覆蓋舊檔案。"""
    if not model_dir.exists():
        return 1
    max_idx = 0
    for p in model_dir.glob(f"{model}_*.csv"):
        try:
            idx = int(p.stem.rsplit("_", 1)[-1])
            max_idx = max(max_idx, idx)
        except ValueError:
            continue
    return max_idx + 1


def write_chunks(df: pd.DataFrame, model_dir: Path, model: str, start_index: int) -> None:
    """將 DataFrame 分成每 CHUNK_SIZE 列一個檔案寫出。"""
    model_dir.mkdir(parents=True, exist_ok=True)
    chunks = [df.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
    for idx, chunk in enumerate(chunks, start=start_index):
        out_path = model_dir / f"{model}_{idx}.csv"
        chunk.to_csv(out_path, index=False)
        print(f"  wrote {out_path.name} ({len(chunk)} rows)")


def main() -> None:
    """讀取 rawdata 目錄下所有 CSV，依型號合併、排序後分檔輸出至 rawdata_rearranged。"""
    if not INPUT_ROOT.exists():
        raise FileNotFoundError(f"Input root not found: {INPUT_ROOT}")

    model_to_paths: dict[str, list[Path]] = {}
    for p in INPUT_ROOT.rglob("*.csv"):
        model = parse_model_name(p)
        if model is None:
            continue
        model_to_paths.setdefault(model, []).append(p)

    if not model_to_paths:
        print("No CSV files found in input root.")
        return

    for model, paths in sorted(model_to_paths.items()):
        print(f"\nProcessing model: {model} ({len(paths)} files)")
        dfs = []
        for p in paths:
            try:
                dfs.append(pd.read_csv(p))
            except Exception as e:
                print(f"  WARNING: failed to read {p.name}: {e}")
        dfs = [d for d in dfs if not d.empty]
        if not dfs:
            print("  all files empty, skipping")
            continue
        df = pd.concat(dfs, ignore_index=True)

        if "Timestamp" not in df.columns:
            print(f"  WARNING: missing 'Timestamp' column for model={model}, skipping")
            continue

        # 將 Timestamp 轉為數值，無法解析的列視為無效並移除
        df["Timestamp"] = pd.to_numeric(df["Timestamp"], errors="coerce")
        df = df.dropna(subset=["Timestamp"])

        if df.empty:
            print("  no valid Timestamp rows, skipping")
            continue

        df = df.sort_values("Timestamp").reset_index(drop=True)
        # 標記資料所屬型號，供後續使用；若原始資料已有 Label 欄位則發出警告
        if "Label" in df.columns:
            print(f"  WARNING: overwriting existing 'Label' column for model={model}")
        df["Label"] = model

        model_dir = OUTPUT_ROOT / model
        start_index = find_next_output_index(model_dir, model)
        write_chunks(df, model_dir, model, start_index=start_index)


if __name__ == "__main__":
    main()
