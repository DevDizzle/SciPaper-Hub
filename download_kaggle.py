import kagglehub
from kagglehub import KaggleDatasetAdapter

# Set the path to the file you'd like to load
file_path = "arxiv-metadata-oai-snapshot.json"

# Load the latest version
df = kagglehub.load_dataset(
  KaggleDatasetAdapter.PANDAS,
  "Cornell-University/arxiv",
  file_path,
)

print(f"Successfully loaded {file_path} into a pandas DataFrame.")
df.to_json(file_path, orient='records', lines=True)
print(f"Successfully saved the DataFrame to {file_path}.")
