import glob
import pandas as pd

# Path to your partial index files (adjust as needed)
index_files = glob.glob("IndexData/index_output/*.csv")
print(f"Found {len(index_files)} index files.")
# Load and combine all
all_parts = [pd.read_csv(f) for f in index_files]
merged_df = pd.concat(all_parts, ignore_index=True)

# Optional: sort by term for cleanliness
#merged_df.sort_values(by=["term", "filename"], inplace=True)

# Save to a single file
merged_df.to_csv("IndexData/inverted_index.csv", index=False)

print(f"Merged {len(index_files)} files into 'inverted_index.csv' with {len(merged_df)} rows.")
