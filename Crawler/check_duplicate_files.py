import os
from collections import defaultdict

def find_duplicate_titles(folder_path, output_file="duplicate_titles_report.txt"):
    title_to_files = defaultdict(list)
    total_txt_files = 0

    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            total_txt_files += 1
            full_path = os.path.join(folder_path, filename)
            try:
                with open(full_path, "r", encoding="utf-8") as f:
                    first_line = f.readline().strip()
                    if first_line.lower().startswith("title:"):
                        title = first_line[len("Title:"):].strip()
                        title_to_files[title].append(filename)
                    else:
                        print(f"⚠️ Skipping {filename}: First line is not a title")
            except Exception as e:
                print(f"❌ Error reading {filename}: {str(e)}")

    # Write results to output file
    with open(output_file, "w", encoding="utf-8") as out:
        out.write("🔍 Duplicate Titles Report\n")
        out.write(f"Total .txt files scanned: {total_txt_files}\n\n")
        
        duplicates_found = False

        for title, files in title_to_files.items():
            if len(files) > 1:
                duplicates_found = True
                out.write(f"🟠 Title: {title}\n")
                for f in files:
                    out.write(f"   - {f}\n")
                out.write("\n")

        if not duplicates_found:
            out.write("✅ No duplicate titles found.\n")

    print(f"\n✅ Scanned {total_txt_files} .txt files")
    print(f"📄 Report written to '{output_file}'")

# Example usage
folder_path = "/home/tmaju002/Desktop/Workspace/github/Wiki_Search_Engine/CrawledData/storage/"  # Replace with your actual folder path
find_duplicate_titles(folder_path)
