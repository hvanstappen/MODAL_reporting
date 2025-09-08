# to view this Streamlit app on a browser, run it with the following command:
#     streamlit run search_semantic.py

import streamlit as st
import os
import numpy as np
import torch
import pandas as pd
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

database_name = "MODAL_data"  # Replace with your database name
collection_name = "collection_name"  # Replace with your collection name


# Load the sentence transformer model for embedding generation
device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)

def open_local_file(file_path):
    try:
        if os.name == 'nt':  # For Windows
            os.startfile(file_path)
        elif os.name == 'posix':  # For macOS or Linux
            os.system(f'xdg-open "{file_path}"')
    except Exception as e:
        st.error(f"Cannot open file: {e}")

# MongoDB client setup
def load_documents_from_mongo(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]

    documents = list(collection.find({})) # change query if needed
    client.close()
    return documents


# Preload the documents and embeddings from MongoDB
def load_embeddings_and_texts(db_name, collection_name):
    documents = load_documents_from_mongo(db_name, collection_name)

    texts = []
    embeddings = []
    doc_ids = []
    extracted_texts = []
    for doc in documents:
        if doc.get('embeddings') and doc['embeddings'][0].get('text_embeddings'):
            embeddings.append(np.array(doc['embeddings'][0]['text_embeddings']))
            texts.append(doc.get('file_path', 'N/A'))  # Or any other text field you wish to show
            doc_ids.append(doc['_id'])
            extracted_text = doc.get('extracted_text', '')
            extracted_texts.append(extracted_text[:100])  # Take first 100 characters

    return texts, np.array(embeddings), doc_ids, extracted_texts


# Semantic search function
def semantic_search(query, embeddings, texts, doc_ids, extracted_texts, top_k=10):
    # Encode the query
    query_embedding = model.encode([query])

    # Compute cosine similarity
    similarities = cosine_similarity(query_embedding, embeddings)

    # Get top_k most similar documents
    sorted_idx = similarities.argsort()[0][-top_k:][::-1]

    # Return the top_k results
    results = []
    for idx in sorted_idx:
        results.append({
            'Text': texts[idx],
            'ObjectId': str(doc_ids[idx]),  # Convert ObjectId to string for better readability
            'Extracted Text': extracted_texts[idx],  # Include the truncated extracted_text
            'Similarity Score': similarities[0][idx]
        })
    return results


# Streamlit UI
def main():
    st.title("Semantic Search for Documents")

    # Input: User's query
    query = st.text_input("Enter your search query:")

    if query:
        # Load documents and embeddings from MongoDB
        texts, embeddings, doc_ids , extracted_texts= load_embeddings_and_texts(database_name, collection_name)

        if len(embeddings) == 0:
            st.error("No valid embeddings found in the database.")
            return

        # Perform semantic search
        search_results = semantic_search(query, embeddings, texts, doc_ids, extracted_texts)

        if search_results:
            st.write(f"Top {len(search_results)} results:")
            for result in search_results:
                file_path = result['Text']

                # Display file information
                # st.write(f"ObjectId: {result['ObjectId']}")
                st.write(f"File Path: {file_path}")
                # st.write(f"Extracted Text (Preview): {result['Extracted Text']}")
                st.write(f"Similarity Score: {result['Similarity Score']:.4f}")

                # Add a button to open the file
                if st.button(f"Open {file_path}"):
                    open_local_file(file_path)

                st.write("---")


        else:
            st.write("No results found.")


if __name__ == "__main__":
    main()
