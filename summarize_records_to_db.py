from collections import Counter
from pymongo import MongoClient
from urllib.parse import unquote
import os
import re
from datetime import datetime  # For enrichment date
# from create_folder_hierarchy import create_folder_records


database_name = "MODAL_data"  # Replace with your database name
collection_name = "collection_name"  # Replace with your collection name


# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Adjust if necessary
db = client[database_name]
collection = db[collection_name]


def escape_regex_chars(text):
    """Escape special regex characters in a string."""
    special_chars = '[\\^$.|?*+(){}'
    return ''.join('\\' + char if char in special_chars else char for char in text)

def extract_ner_data(enrichments, field_name):
    """Extracts NER data from any enrichment element that contains it."""
    if not isinstance(enrichments, list):
        print(f"Warning: Expected a list for enrichments, got {type(enrichments).__name__}")
        return []

    extracted_values = []
    for enrichment in enrichments:
        if isinstance(enrichment, dict) and field_name in enrichment:
            value = enrichment[field_name]
            if isinstance(value, list):
                extracted_values.extend(value)
            else:
                extracted_values.append(value)  # Wrap single value as list
    return extracted_values


def summarize_records():
    all_folder_docs = list(collection.find({'file_name': 'folder_summary','enrichments': {'$exists': 0}}))
    print(len(all_folder_docs))
    counter = 0
    total = len(all_folder_docs)

    all_folder_paths = []
    for folder_doc in all_folder_docs:
        counter = counter + 1
        print(f"Folder {counter} of {total}")
        # Get the file_path and split it into levels
        # file_path = doc.get("file_path", "").split(path_start_to_ignore)[-1]
        folder_path = folder_doc.get("file_path")
        print(folder_path)
        all_folder_paths.append(folder_path)

    for path in all_folder_paths:
        print(f"\nProcessing Path: {path}")
        
        # Escape special characters in path and create a safe regex pattern
        escaped_path = escape_regex_chars(path)
        regex = f"^{escaped_path}[^/]*$"

        # Query MongoDB for documents where the `file_path` matches the regex
        docs = list(collection.find({
            "file_path": {"$regex": regex},
            "enrichments": {"$exists": True}
        }))
        # docs = list(collection.find({"file_path": {"$regex": regex}}))

        # Summarize the number of documents found for this path
        print(f"Found {len(docs)} documents for {path}")

        # get the metadata
        NER_persons = []
        NER_organisations = []
        NER_locations = []
        NER_miscellaneous = []
        Topic_representation = []
        Topic_label = []

        for doc in docs:
            enrichments = doc.get("enrichments", [])

            ner_persons = extract_ner_data(enrichments, "NER_persons")
            NER_persons = NER_persons + ner_persons

            ner_organisations = extract_ner_data(enrichments, "NER_organisations")
            NER_organisations = NER_organisations + ner_organisations

            ner_locations = extract_ner_data(enrichments, "NER_locations")
            NER_locations = NER_locations + ner_locations

            ner_miscellaneous = extract_ner_data(enrichments, "NER_miscellaneous")
            NER_miscellaneous = NER_miscellaneous + ner_miscellaneous

            topic_representation = extract_ner_data(enrichments, "Topic_representation")
            Topic_representation = Topic_representation + topic_representation

            topic_label = extract_ner_data(enrichments, "Topic_label")
            Topic_label = Topic_label + topic_label



        # get top20
        NER_persons_counter = Counter(NER_persons)
        NER_persons_top20 = NER_persons_counter.most_common(20)
        NER_organisations_counter = Counter(NER_organisations)
        NER_organisations_top20 = NER_organisations_counter.most_common(20)
        NER_locations_counter = Counter(NER_locations)
        NER_locations_top20 = NER_locations_counter.most_common(20)
        NER_miscellaneous_counter = Counter(NER_miscellaneous)
        NER_miscellaneous_top20 = NER_miscellaneous_counter.most_common(20)
        Topic_representation_counter = Counter(Topic_representation)
        Topic_representation_top20 = Topic_representation_counter.most_common(20)
        Topic_label_counter = Counter(Topic_label)
        Topic_label_top20 = Topic_label_counter.most_common(20)


                # Prepare the enrichment record
        enrichment_record = {
            "model_used": "summarizer",
            "enrichment_date": datetime.now().isoformat(),
            "NER_persons": [person for person, _ in NER_persons_top20],
            "NER_organisations": [organisation for organisation, _ in NER_organisations_top20],
            "NER_locations": [location for location, _ in NER_locations_top20],
            "NER_miscellaneous": [miscellaneous for miscellaneous, _ in NER_miscellaneous_top20],
            "TOPIC_representation": [representation for representation, _ in Topic_representation_top20],
            "TOPIC_label": [label for label, _ in Topic_label_top20]
        }

        # Append the enrichment to the `enrichments` array
        collection.update_one(
            {"file_path": path},
            {
                "$push": {"enrichments": enrichment_record},  # Append to enrichments array
                "$set": {"file_name": "folder_summary"}  # Ensure correct file_name is set
            },
            upsert=False  # Create the record if it doesn't exist
        )



# paths = create_folder_records()
# print(f"Paths: {paths}")
print("\n\nStarting summarization process... This may take a while. Please be patient. :)")
summarize_records()