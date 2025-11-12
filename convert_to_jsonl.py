import pandas as pd
import argparse
import sys

def convert_parquet_to_jsonl(source_gcs_path, dest_gcs_path):
    """Reads a parquet file from GCS and writes it back as JSONL."""
    try:
        print(f"Reading from {source_gcs_path}")
        # Ensure gcsfs is used for GCS paths
        df = pd.read_parquet(source_gcs_path, engine='pyarrow')
        print(f"Writing {len(df)} uncompressed records to {dest_gcs_path}")
        # Write uncompressed JSONL
        df.to_json(dest_gcs_path, orient='records', lines=True)
        print("Conversion complete.")
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Parquet from GCS to JSONL in GCS.")
    parser.add_argument("source", help="Source GCS path for the Parquet file (e.g., gs://bucket/file.parquet)")
    parser.add_argument("dest", help="Destination GCS path for the JSONL file (e.g., gs://bucket/file.json)")
    args = parser.parse_args()
    convert_parquet_to_jsonl(args.source, args.dest)