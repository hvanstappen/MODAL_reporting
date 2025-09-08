#  This script creates a folder and writes the extracted text of each document to a text file in the folder.
#  This can be use to create a document set for a RAG app

import pymongo
import os

# MongoDB connection details
mongo_uri = "mongodb://localhost:27017/"
DB_NAME = "MODAL_testdata"
COLLECTION_NAME = "LH_HH_71_Hemmerechts"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# CSV file name
folder = f"data/files_as_txt/" + COLLECTION_NAME

# Create the folder if it doesn't exist
os.makedirs(folder, exist_ok=True)

# properties_list = ["_id", "file_name", "extracted_text_corrected"]

for document in collection.find({'word_count': {'$gte': 0 }}):
    id = str(document["_id"])
    file_name = document.get("file_name", id) + ".txt"  # Added .txt extension
    file_content = document.get("extracted_text", "")
    
    # Create the full file path
    file_path = os.path.join(folder, file_name)
    print(f'Writing {file_name} to {folder}')
    
    # Write the content to the file
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(file_content)
    except Exception as e:
        print(f"Error writing file {file_name}: {str(e)}")