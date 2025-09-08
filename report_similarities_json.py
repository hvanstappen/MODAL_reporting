import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from pymongo import MongoClient
import json

database_name = "MODAL_sourcedata"  # Replace with your database name
collection_name = "LH_JPearce"  # Replace with your collection name
output_filename = f"data/similarities/{collection_name}_sim.json"
threshold = 0.98  # Adjust the similarity threshold as needed

def find_similar_documents(db_name, collection_name, output_file="similar_documents.json", similarity_threshold=0.9):
    """
    Finds and stores pairs of similar documents in a JSON file.

    Args:
        db_name (str): The name of the MongoDB database.
        collection_name (str): The name of the MongoDB collection.
        output_file (str): The name of the JSON file to write the results to.
        similarity_threshold (float): The minimum cosine similarity to consider documents similar.
    """

    with MongoClient('mongodb://localhost:27017/') as client:
        db = client[db_name]
        collection = db[collection_name]

        documents = list(collection.find({}))

        if not documents:
            print("No documents found in the collection.")
            return

        embeddings = []
        doc_ids = []
        doc_paths = {}
        counter = 0

        for doc in documents:
            if doc.get('embeddings') and doc['embeddings'][0].get('text_embeddings'):
                embeddings.append(doc['embeddings'][0]['text_embeddings'])
                doc_ids.append(str(doc['_id']))  # Convert ObjectId to string
                doc_paths[str(doc['_id'])] = doc.get('file_path')
            # else:
            #     print(f"Skipping document {doc['_id']} (No valid embeddings)")

        if not embeddings:
            print("No valid embeddings found in the collection.")
            return

        # Convert embeddings to NumPy array for efficiency
        embeddings = np.array(embeddings)

        # Compute cosine similarity for all pairs at once
        similarity_matrix = cosine_similarity(embeddings)

        num_docs = len(doc_ids)
        results = {doc_id: {"id": doc_id, "file_path": doc_paths.get(doc_id), "similar_documents": []} for doc_id in
                   doc_ids}

        for i in range(num_docs):
            counter += 1
            for j in range(i + 1, num_docs):
                similarity = similarity_matrix[i, j]
                if similarity >= similarity_threshold:
                    doc1_id, doc2_id = doc_ids[i], doc_ids[j]

                    results[doc1_id]["similar_documents"].append({
                        "id": doc2_id,
                        "file_path": doc_paths.get(doc2_id),
                        "similarity_score": similarity
                    })
                    results[doc2_id]["similar_documents"].append({
                        "id": doc1_id,
                        "file_path": doc_paths.get(doc1_id),
                        "similarity_score": similarity
                    })

                    print(f"Found similarity: {doc1_id} ↔ {doc2_id} with score {similarity:.4f} ({counter}/{num_docs})")

        # Sort similar documents by similarity score
        for doc_id in results:
            results[doc_id]["similar_documents"].sort(key=lambda x: x["similarity_score"], reverse=True)

        with open(output_file, 'w') as f:
            json.dump(results, f, indent=4)

        print(f"Similar document information written to '{output_file}'")




find_similar_documents(database_name, collection_name, output_filename, threshold)
