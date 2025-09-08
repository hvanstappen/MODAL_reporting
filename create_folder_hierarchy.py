from pymongo import MongoClient


database_name = "MODAL_data"  # Replace with your database name
collection_name = "collection_name"  # Replace with your collection name


# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Adjust if necessary
db = client[database_name]
collection = db[collection_name]

def create_folder_records():
    all_docs = list(collection.find())
    all_folder_paths = []
    for doc in all_docs:
        # Get the file_path and split it into levels
        # file_path = doc.get("file_path", "").split(path_start_to_ignore)[-1]
        file_path = doc.get("file_path", "")
        path_parts = file_path.split("/")

        # Construct paths for all folder levels and add them to all_folder_paths
        for i in range(1, len(path_parts)):
            folder_path = "/".join(path_parts[:i]) + "/"
            if folder_path not in all_folder_paths:
                all_folder_paths.append(folder_path)

    all_folder_paths = sorted(all_folder_paths, reverse=True)
    for folder in all_folder_paths:
        folder_record = {
            "file_name": "folder_summary",  # Fixed value
            "file_path": folder  # Path of the folder
        }
        # Insert the record into the collection
        collection.insert_one(folder_record)
        print(f"Inserted record: {folder_record}")


    return all_folder_paths

create_folder_records()