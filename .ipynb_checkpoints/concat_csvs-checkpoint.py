# concat_csvs.py
import pandas as pd
from pathlib import Path

ENCODINGS_TRY = ["utf-8-sig", "utf-16", "utf-16le", "utf-16be", "cp1252", "latin-1"]

def read_csv_robust(path: Path):
    """
    Try multiple encodings and let pandas sniff the delimiter.
    Returns a DataFrame or raises the last exception.
    """
    last_err = None
    for enc in ENCODINGS_TRY:
        try:
            # sep=None lets pandas sniff ',', ';', '\t', etc. (requires engine='python')
            df = pd.read_csv(path, sep=None, engine="python", encoding=enc)
            return df
        except Exception as e:
            last_err = e
            continue
    raise last_err

def main():
    data_dir = Path("data")
    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "concatenated.csv"

    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {data_dir.resolve()}")
        return

    print(f"Found {len(csv_files)} CSV files. Concatenating...")

    frames = []
    successes = []
    failures = []

    for f in csv_files:
        try:
            df = read_csv_robust(f)
            # add source filename for tracing
            df.insert(0, "source_file", f.name)
            frames.append(df)
            successes.append(f.name)
        except Exception as e:
            failures.append((f.name, str(e)))

    if not frames:
        print("No files could be read successfully. See errors below:")
        for name, msg in failures:
            print(f"  - {name}: {msg}")
        return

    # Align columns by name automatically; ignore_index to reindex from 0
    combined = pd.concat(frames, ignore_index=True, sort=False)

    # Save as UTF-8 with BOM to play nicely with Excel
    combined.to_csv(out_file, index=False, encoding="utf-8-sig")

    print(f"✅ Saved concatenated CSV to: {out_file.resolve()}")
    print(f"   Rows: {len(combined):,} | Columns: {len(combined.columns)}")
    if failures:
        print("\nSome files were skipped due to read errors:")
        for name, msg in failures:
            print(f"  - {name}: {msg}")

if __name__ == "__main__":
    main()
