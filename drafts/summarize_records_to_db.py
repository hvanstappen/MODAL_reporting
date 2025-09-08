from collections import Counter
from pymongo import MongoClient
from urllib.parse import unquote
import os

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Adjust if necessary
db = client["MODAL_testdata"]  # Replace with your actual database name
collection = db["AMSAB_archieven_d_2013_003"]  # Replace with your actual collection name


# Function to extract the folder path
def extract_folder_path(file_path):
    decoded_path = unquote(file_path)
    return os.path.dirname(decoded_path) if '.' in os.path.basename(
        decoded_path) else decoded_path  # Handle folder records correctly


# Function to calculate folder depth
def get_folder_depth(folder_path):
    return folder_path.count(os.path.sep)


# Function to summarize metadata
def summarize_metadata(docs, folder):
    ner_persons = Counter()
    ner_organisations = Counter()
    ner_locations = Counter()
    ner_miscellaneous = Counter()
    topic_representations = Counter()
    topic_labels = Counter()

    for doc in docs:
        for enrichment in doc.get("enrichments", []):
            ner_persons.update(enrichment.get("NER_persons", []))
            ner_organisations.update(enrichment.get("NER_organisations", []))
            ner_locations.update(enrichment.get("NER_locations", []))
            ner_miscellaneous.update(enrichment.get("NER_miscellaneous", []))
            topic_representations.update(enrichment.get("Topic_representation", []))

            if "Topic_label" in enrichment:
                if isinstance(enrichment["Topic_label"], list):
                    topic_labels.update(enrichment["Topic_label"])
                else:
                    topic_labels[enrichment["Topic_label"]] += 1

    return {
        "file_name": "SUMMARY_RECORD",
        "file_path": folder,
        "file_mimetype": "text/summary",
        "enrichments": [
            {
                "model_used": "Aggregated Summary",
                "NER_persons": [item[0] for item in ner_persons.most_common(10)],
                "NER_organisations": [item[0] for item in ner_organisations.most_common(10)],
                "NER_miscellaneous": [item[0] for item in ner_miscellaneous.most_common(10)],
                "NER_locations": [item[0] for item in ner_locations.most_common(10)]
            },
            {
                "model_used": "Aggregated Summary",
                "Topic_representation": [item[0] for item in topic_representations.most_common(10)],
                "Topic_label": [item[0] for item in topic_labels.most_common(10)]
            }
        ]
    }


# Function to process summaries from bottom to top
def process_folders_bottom_up():
    processed_folders = set()

    # Fetch all documents
    all_docs = list(collection.find())

    # Extract all unique folders
    folder_map = {}
    for doc in all_docs:
        folder = extract_folder_path(doc["file_path"])
        if folder not in folder_map:
            folder_map[folder] = []
        folder_map[folder].append(doc)

    # Sort folders by depth (deepest first)
    folder_levels = sorted(folder_map.keys(), key=get_folder_depth, reverse=True)

    for folder in folder_levels:
        if folder in processed_folders:
            continue

        docs = folder_map[folder]

        # Include summaries of direct subfolders in the metadata collection
        subfolder_summaries = list(
            collection.find({"file_path": {"$regex": f"^{folder}/"}, "file_name": "SUMMARY_RECORD"}))
        docs.extend(subfolder_summaries)

        # Create and insert summary record
        summary_record = summarize_metadata(docs, folder)
        collection.insert_one(summary_record)
        processed_folders.add(folder)
        print(f"Inserted summary for: {folder}")


process_folders_bottom_up()
print("Bottom-up metadata summarization complete.")
