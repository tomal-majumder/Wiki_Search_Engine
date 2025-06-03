import glob
import os
import json
import base64
from pymongo import MongoClient

def cut_the_article(body):
    chunked_string = ""
    counter = 0

    # Process up to the first 3 lines of the body (or fewer if body is shorter)
    for i in range(min(3, len(body))):
        line = body[i]
        for char in line:
            chunked_string += char
            if char == ".":
                counter += 1
            if counter == 2:
                break
        if counter == 2:
            break

    return chunked_string.strip()

# Connect with MongoDB
mongo_client = MongoClient("mongodb://127.0.0.1:27017")
print("Connection Successful")
# Create the database named "ir"
db = mongo_client.ir

if "wikipedia" in db.list_collection_names():
    db.wikipedia.drop()
    print("Dropped existing 'wikipedia' collection")
    
# Input file directories - adjust these paths to match your environment
text_dir_path = "/home/tmaju002/Desktop/Workspace/github/Wiki_Search_Engine/CrawledData/storage/*.txt"
image_dir = "/home/tmaju002/Desktop/Workspace/github/Wiki_Search_Engine/CrawledData/storage/images"

# Get all text files
file_list = glob.glob(text_dir_path)

for file_path in file_list:
    if os.path.isfile(file_path):
        try:
            # Process each file
            with open(file_path, "r", encoding="utf-8") as file:
                all_lines = file.readlines()
            
            # First line contains the title
            title_line = all_lines[0]
            title = title_line.replace("Title: ", "").replace(" - Wikipedia", "").strip()
            
            # Rest of the content is the body
            body = all_lines[1:]
            chunked_body = cut_the_article(body)
            # Get filename components
            filename = os.path.basename(file_path)
            file_id = filename.split(".txt")[0]  # This is the hash code
            
            # Create URL from title
            wiki_title = title.replace(" ", "_")
            url = f"https://en.wikipedia.org/wiki/{wiki_title}"
            
            # Look for associated images
            image_files = []
            for i in range(0, 10):  # Check for up to 10 images
                img_path = os.path.join(image_dir, f"{file_id}-{i}.jpg")
                if os.path.isfile(img_path):
                    image_files.append(img_path)
            
            # Create image data
            image_data = []
            for i, img_path in enumerate(image_files):
                image_info = {
                    "image_id": f"{file_id}-{i}",
                    "image_path": img_path,
                    # You can store the actual image data if needed:
                    # "image_data": base64.b64encode(open(img_path, "rb").read()).decode('utf-8')
                    # Or just store the reference path and load images separately when needed
                }
                image_data.append(image_info)
            
            # Insert data
            json_data = {
                "docId": title,
                "body": chunked_body, 
                "filename": filename,
                "file_id": file_id,
                "url": url,
                "images": image_data,
                "image_count": len(image_data)
            }
            
            # Collection name is "wikipedia"
            rec_id = db.wikipedia.insert_one(json_data)
            
            print(f"Inserted document '{title}' with {len(image_data)} images. ID: {rec_id.inserted_id}")
            
        except Exception as e:
            print(f"An exception occurred with file {filename}: {str(e)}")

print("Processing complete")
mongo_client.close()