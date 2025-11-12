import pandas as pd
import argparse
import sys
import json
import os
from google.cloud.storage import Client, aiplatform
from common.config import get_settings
from service.embed_vertex import VertexEmbeddingClient

def generate_embeddings(source_gcs_path, dest_gcs_path):
    """Reads records, generates embeddings, and saves to JSONL."""
    try:
        # Init clients
        print("Initializing clients...")
        settings = get_settings()
        aiplatform.init(project=settings.project_id, location=settings.region)
        embed_client = VertexEmbeddingClient()
        print("Clients initialized.")

        print(f"Reading from {source_gcs_path}")
        df = pd.read_parquet(source_gcs_path, engine='pyarrow')
        print(f"Read {len(df)} records.")

        print(f"Generating embeddings for {len(df)} records...")
        abstracts = df["abstract"].tolist()
        embeddings = embed_client.embed_batch(abstracts)
        print("Embeddings generated.")

        print(f"Writing {len(df)} embeddings to {dest_gcs_path}")
        
        # Use a temporary local file for writing JSONL
        local_tmp_file = "embeddings.jsonl"
        with open(local_tmp_file, "w") as f:
            for i, row in df.iterrows():
                # Ensure the ID is a string
                record_id = str(row["base_id"])
                embedding_list = embeddings[i]
                
                if not record_id:
                    print(f"Warning: Skipping record at index {i} due to empty ID.", file=sys.stderr)
                    continue

                output_record = {
                    "id": record_id,
                    "embedding": embedding_list
                }
                f.write(json.dumps(output_record) + "\n")

        print(f"Uploading {local_tmp_file} to {dest_gcs_path}...")
        storage_client = Client()
        bucket_name, blob_name = dest_gcs_path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_tmp_file)
        print("Upload complete.")
        
        os.remove(local_tmp_file)
        print(f"Removed temporary file {local_tmp_file}.")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate embedding JSONL file from a Parquet file.")
    parser.add_argument("source", help="Source GCS path for the Parquet file (e.g., gs://bucket/file.parquet)")
    parser.add_argument("dest", help="Destination GCS path for the JSONL file (e.g., gs://bucket/file.json)")
    args = parser.parse_args()
    generate_embeddings(args.source, args.dest)
