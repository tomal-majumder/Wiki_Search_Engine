import glob
import os 
import json
from pymongo import MongoClient

#connect with mongodb
mongo_client = MongoClient("mongodb://127.0.0.1:27017")
print("Connection Successful")
#create the database named "ir
db = mongo_client.ir


# input file directory
# change it according to your machine
dir_path = "/home/tmaju002/Desktop/Workspace/Projects/Wiki_Search_Engine/CrawledData/storage/*.txt"
file_list = glob.glob(dir_path)

for file_path in file_list:
	if (os.path.isfile(file_path)):
		try:
			#process each file and store in mongodb
			#mongodb collection name is wikipedia
			file = open(file_path, "r")
			all_lines = file.readlines()
			all_lines.pop(0)
			body = all_lines
			filename = os.path.basename(file_path)
			title = filename.split("_-_Wikipedia.txt")[0].replace("cs242", ":")
			url = "https://en.wikipedia.org/wiki/" + title

			#insert data
			json_data = {"docId": title, "body": body, "filename": filename, "url": url}
			# collection name is "wikipedia"
			rec_id = db.wikipedia.insert_one(json_data)
		
			print("rec_id: ", rec_id)
		except:
			print("An exception occurred!")
		file.close()

print("done")
mongo_client.close()
