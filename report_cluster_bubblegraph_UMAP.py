import numpy as np
import umap
import plotly.express as px
import pandas as pd
from pymongo import MongoClient
import datetime
from sklearn.preprocessing import StandardScaler


def visualize_document_similarities_interactive(db_name, collection_name, output_file="document_similarity.html"):
    """
    Visualizes document similarities using UMAP and Plotly, with MIME type as color and date as hover info.

    Args:
        db_name (str): The name of the MongoDB database.
        collection_name (str): The name of the MongoDB collection.
        output_file (str): The name of the HTML file to save the plot to.
    """

    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]

    documents = collection.find(
        {
            "$and": [
                {"$or": [{"file_mimetype": "application/msword"},
                         {"file_mimetype": "application/vnd.wordperfect; version=5.1"},
                         {"file_mimetype": "application/vnd.wordperfect; version=5.0"},
                         {"file_mimetype": "application/rtf"},
                         {"file_mimetype": "application/pdf"},
                         {"file_mimetype": "application/vnd.ms-works"},
                         {"file_mimetype": "application/x-tika-msoffice"},
                         {"file_mimetype": "application/vnd.oasis.opendocument.tika.flat.document"},
                         {"file_mimetype": "application/vnd.ms-word.document.macroenabled.12"},
                         {"file_mimetype": "application/msword2"},
                         {"file_mimetype": "application/vnd.wordperfect"},
                         {"file_mimetype": "application/x-mspublisher"},
                         {"file_mimetype": "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
                         {"file_mimetype": "application/vnd.openxmlformats-officedocument.presentationml.slideshow"},
                         {"file_mimetype": "application/vnd.oasis.opendocument.text"},
                         {"file_mimetype": "application/vnd.oasis.opendocument.presentation"},
                         {"file_mimetype": "message/rfc822"},
                         {"file_mimetype": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
                         {"file_mimetype": "message/x-emlx"},
                         {"file_mimetype": "application/vnd.ms-powerpoint"},
                         {"file_mimetype": "application/vnd.wordperfect; version=6.x"}]},
                {"word_count": {"$gte": 20}}
            ]
        },
        {"_id": 1, "embeddings": 1, "file_path": 1, "creation_date": 1, "word_count": 1, "file_mimetype": 1, "extracted_text": 1}
    )

    embeddings = []
    doc_ids = []
    doc_file_paths = []
    doc_dates = []
    doc_word_counts = []
    doc_mime_types = []
    doc_extracted_texts = []

    for doc in documents:
        if doc.get('embeddings') and doc['embeddings'][0].get('text_embeddings'):
            embeddings.append(np.array(doc['embeddings'][0]['text_embeddings']))
            doc_ids.append(doc['_id'])
            doc_file_paths.append(doc.get('file_path', 'N/A'))
            doc_word_counts.append(doc.get('word_count', 0))
            doc_extracted_texts.append(doc.get('extracted_text', 'N/A')[:100])
            doc_mime_types.append(doc.get('file_mimetype', 'Unknown'))

            # Convert 'creation_date' to YYYY-MM-DD format
            date_created = doc.get('creation_date')
            if isinstance(date_created, str):
                try:
                    date_obj = datetime.datetime.fromisoformat(date_created.replace('Z', '+00:00'))
                    doc_dates.append(date_obj.strftime('%Y-%m-%d'))
                except ValueError:
                    doc_dates.append("Unknown")
            elif isinstance(date_created, datetime.datetime):
                doc_dates.append(date_created.strftime('%Y-%m-%d'))
            else:
                doc_dates.append("Unknown")

    if not embeddings:
        print("No documents with valid embeddings found.")
        client.close()
        return

    # Normalize embeddings for better clustering
    embeddings = np.array(embeddings)
    embeddings = StandardScaler().fit_transform(embeddings)

    # Add small random noise to prevent numerical issues
    embeddings += np.random.normal(0, 0.01, embeddings.shape)

    # Apply UMAP with adjusted parameters
    reducer = umap.UMAP(
        n_neighbors=50,  # More neighbors for better global structure
        min_dist=0.2,  # Adjust separation between clusters
        metric='cosine',  # Select 'euclidian' or 'cosine' if needed
        random_state=42,
        init='random'  # Avoid spectral initialization issues
    )
    reduced_embeddings = reducer.fit_transform(embeddings)

    # Create DataFrame for Plotly
    df = pd.DataFrame({
        'Dimension 1': reduced_embeddings[:, 0],
        'Dimension 2': reduced_embeddings[:, 1],
        'File Path': doc_file_paths,
        'Word Count': doc_word_counts,
        'MIME Type': doc_mime_types,
        'Creation Date': doc_dates,  # Stored as 'YYYY-MM-DD'
        'Text': doc_extracted_texts
    })

    # Generate interactive plot
    fig = px.scatter(
        df,
        x='Dimension 1',
        y='Dimension 2',
        hover_name='File Path',
        hover_data={'MIME Type': True, 'Word Count': True, 'Creation Date': True, 'Text': True},
        color='MIME Type',  # Color by MIME type
        size='Word Count',
        size_max=100,
        title=f"{collection_name} Document Similarity Visualization (UMAP)"
    )

    fig.write_html(output_file)
    print(f"Interactive plot saved to '{output_file}'")

    client.close()


if __name__ == "__main__":
    database_name = "MODAL_testdata"
    collection_name = "LH_JPearce"
    output_filename = f"/home/henk/DATABLE/1_Projecten/2024_MODAL/3_Data/similarities/{collection_name}_document_similarity_texts_UMAP.html"

    visualize_document_similarities_interactive(database_name, collection_name, output_filename)
