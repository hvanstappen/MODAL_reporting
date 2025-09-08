import numpy as np
from sklearn.manifold import TSNE
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import pandas as pd
from pymongo import MongoClient
import datetime


def visualize_document_similarities_interactive(db_name, collection_name, output_file="document_similarity.html"):
    """
    Visualizes document similarities using t-SNE and Plotly, with MIME type as color and formatted date in hover.

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
        {"_id": 1, "embeddings": 1, "file_path": 1, "creation_date": 1, "language": 1, "word_count": 1,
         "file_mimetype": 1, "extracted_text": 1}
    )

    embeddings = []
    doc_ids = []
    doc_file_paths = []
    doc_word_counts = []
    doc_mime_types = []
    doc_dates = []
    doc_extracted_texts = []

    for doc in documents:
        if doc.get('embeddings') and doc['embeddings'][0].get('text_embeddings'):
            embeddings.append(np.array(doc['embeddings'][0]['text_embeddings']))
            doc_ids.append(doc['_id'])
            doc_file_paths.append(doc.get('file_path', 'N/A'))
            doc_word_counts.append(doc.get('word_count', 0))
            doc_mime_types.append(doc.get('file_mimetype', 'Unknown'))  # Default to 'Unknown' if missing
            doc_extracted_texts.append(doc.get('extracted_text', 'N/A')[:100])

            # Extract and format the date
            date_created = doc.get('creation_date')
            formatted_date = None
            if isinstance(date_created, str):
                try:
                    formatted_date = datetime.datetime.fromisoformat(date_created.replace('Z', '+00:00')).strftime(
                        '%Y-%m-%d')
                except ValueError:
                    formatted_date = "Unknown"
            elif isinstance(date_created, datetime.datetime):
                formatted_date = date_created.strftime('%Y-%m-%d')
            else:
                formatted_date = "Unknown"

            doc_dates.append(formatted_date)

    if not embeddings:
        print("No documents with valid embeddings found.")
        client.close()
        return

    embeddings = np.array(embeddings)
    tsne = TSNE(n_components=2, random_state=42, perplexity=50, learning_rate=300)
    reduced_embeddings = tsne.fit_transform(embeddings)

    df = pd.DataFrame({
        'Dimension 1': reduced_embeddings[:, 0],
        'Dimension 2': reduced_embeddings[:, 1],
        'File Path': doc_file_paths,
        'Word Count': doc_word_counts,
        'MIME Type': doc_mime_types,  # Use MIME type as categorical color
        'Date Created': doc_dates,  # Formatted as YYYY-MM-DD
        'Text': doc_extracted_texts
    })

    fig = px.scatter(
        df,
        x='Dimension 1',
        y='Dimension 2',
        hover_name='File Path',
        hover_data={'Word Count': True, 'MIME Type': True, 'Date Created': True, 'Text': True},  # Date formatted correctly
        color='MIME Type',  # Categorical color
        size='Word Count',
        size_max=100,
        title=f"{collection_name} Document Similarity Visualization (t-SNE)"
    )

    fig.write_html(output_file)
    print(f"Interactive plot saved to '{output_file}'")

    client.close()


if __name__ == "__main__":
    database_name = "MODAL_testdata"
    collection_name = "LH_JPearce"
    output_filename = f"/home/henk/DATABLE/1_Projecten/2024_MODAL/3_Data/similarities/{collection_name}_document_similarity_texts_tSNE.html"
    visualize_document_similarities_interactive(database_name, collection_name, output_filename)
