from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
import torch
from pymongo import MongoClient
from datetime import datetime
import logging
import re

database_name = "MODAL_data"  # Replace with your database name
collection_name = "collection_name"  # Replace with your collection name

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Adjust if necessary
db = client[database_name]
collection = db[collection_name]

device = "cuda:0" if torch.cuda.is_available() else "cpu"

# model_name = "google/gemma-2-2b-it"
model_name = "google/gemma-3-1b-it"
# model_name = "google/gemma-3-4b-it"

# Load model in 8-bit precision
model = AutoModelForCausalLM.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)

pipe = pipeline("text-generation", model=model_name, torch_dtype=torch.bfloat16, max_new_tokens=100, device=device)


def summarize_summaries(summaries): #concatenate
    summarized = ""
    file_count = 0
    error_count = 0

    message = [
        {
            "role": "system",
            "content": "Geef een antwoord in een korte zin. "},
        {
            "role": "user",
            "content": "Hieronder volgt een reeks samenvattingen:\n{}\nVat ze samen tot een definitieve, geconsolideerde samenvatting van de belangrijkste thema's.".format(
                summaries)
        }
    ]

    prompt = pipe.tokenizer.apply_chat_template(message, tokenize=False, add_generation_prompt=True)
    try:
        outputs = pipe(
            prompt,
            do_sample=True,
            temperature=0.1,
            top_k=20,
            top_p=0.1,
        )

        if not outputs or "generated_text" not in outputs[0]:
            logging.error(f"Unexpected model output: {outputs}")
            # continue  # Skip to the next record

        summarized = outputs[0]["generated_text"][len(prompt):].replace('#', '')
        # extracted_response = extract_model_reply(response)
        # print(f"\nExtracted response: {summarized}")
        file_count += 1
        # print(f"Files processed: {file_count}")
        # print("--------------------------------")
    except RuntimeError as e:
        logging.error(f"Error processing text from file: {e}")
        error_count += 1
        print(f"Errors encountered: {error_count}")
        # continue

    return summarized

def summarize_records():
    # Select all records representing a folder and missing a summary
    all_folder_docs = list(collection.find({"$and": [{"file_name":"folder_summary"},{"enrichments.summary":{"$exists":0}}]}))
    # all_folder_docs = list(collection.find({'file_path': '/media/henk/LaCie/2025_MODAL/LH/UitgeverijVrijdag/Acq_lh_179_Uitgeverij Vrijdag N.V/Uitgeverij Vrijdag N.V/Uitgeverij Vrijdag - Hoofdmap/vrijdag/_W.I.P/C/Caron, Bart/Vanop de frontlijn (Caron, Bart & Redig, Guy)/DRUKKLAAR/'}))
    print(len(all_folder_docs))
    all_folder_paths = []
    for folder_doc in all_folder_docs:
        folder_path = folder_doc.get("file_path","")
        print("paths of records representing a folder:")
        print(folder_path)
        all_folder_paths.append(folder_path)

    for path in all_folder_paths:
        print(f"\nProcessing Path: {path}")

        # Escape special regex characters in the path
        escaped_path = re.escape(path)
        # Match file_path strings that start with 'path' and do not contain additional forward slashes
        regex = f"^{escaped_path}([^/]*|[^/]+/)$"

        # Query MongoDB for documents
        docs = list(collection.find({
              "file_path": {"$regex": regex},
              "enrichments.summary": {"$exists": True}
          }))

        # Summarize the number of documents found for this path
        print(f"Found {len(docs)} documents for {path}")

        # get the summaries
        summary_list = "" #concatenate all summaries
        print("selecting summaries:")
        summaries_counter = 0
        for doc in docs:
            enrichments = doc.get("enrichments", [])
            file_name = doc.get("file_name")

            for enrichment in enrichments:
                if "summary" in enrichment:
                    summary = enrichment["summary"]
                    if len(summary) > 10:  # summary must exist
                        summaries_counter = summaries_counter + 1
                        if summaries_counter < 30:
                            summary_list += summary + "\n "
                            print(f"summary {summaries_counter}: {summary}")
                        else:
                            # print("reached max of 30 summaries to summarize. \n")
                            continue
                    else:
                        print("empty summary, skipping..")
                        continue

        summary_list_short = summary_list[:5000]  # limit to max X characters

        if summary_list_short == "":
            summarized = ""
        elif summaries_counter == 1:  # don't summarize if there's only one summary
            summarized = summary_list_short
        else:
            summarized = summarize_summaries(summary_list_short)
        print(f"\nsummarized: {summarized}")

        # Prepare the enrichment record
        enrichment_record = {
            "model_used": model_name,
            "enrichment_date": datetime.now().isoformat(),
            "summary": summarized
        }


        # Append the enrichment to the `enrichments` array
        collection.update_one(
            {"file_path": path},
            {
                "$push": {"enrichments": enrichment_record},
                "$set": {"file_name": "folder_summary"}
            },
            upsert=False  # Create the record if it doesn't exist
        )


summarize_records()