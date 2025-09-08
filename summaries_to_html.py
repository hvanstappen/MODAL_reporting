from pymongo import MongoClient
import os
import html

database_name = "MODAL_data"
collection_name = "collection_name"

def extract_field(enrichments, field_name):
    """Extracts field data from enrichment elements, normalizing strings/lists."""
    for enrichment in enrichments:
        if field_name in enrichment:
            value = enrichment[field_name]
            if isinstance(value, list):
                return value
            elif value:  # If it's a non-empty string or value
                return [value]
            else:
                return []
    # return [] if field_name.startswith("NER") or field_name in ["TOPIC_representation", "TOPIC_label"] else ""
    return [] if field_name.startswith("NER") or field_name in ["TOPIC_representation", "TOPIC_label"] else ""


def build_hierarchy():
    """Builds a hierarchical dictionary representing the file and folder structure."""
    client = MongoClient("mongodb://localhost:27017/")
    db = client[database_name]
    collection = db[collection_name]

    all_docs = list(collection.find())
    hierarchy = {}
    metadata_map = {}

    for doc in all_docs:
        file_path = os.path.normpath(doc["file_path"].strip("/"))
        enrichments = doc.get("enrichments", [])
        # extracted_text = doc.get("extracted_text", "")[300]

        metadata_map[file_path] = {
            "NER_persons": extract_field(enrichments, "NER_persons"),
            "NER_organisations": extract_field(enrichments, "NER_organisations"),
            "NER_locations": extract_field(enrichments, "NER_locations"),
            "NER_miscellaneous": extract_field(enrichments, "NER_miscellaneous"),
            "TOPIC_representation": extract_field(enrichments, "TOPIC_representation"),
            "TOPIC_label": extract_field(enrichments, "TOPIC_label"),
            "summary": extract_field(enrichments, "summary"),
            "sender_email": doc.get("sender_email", []),
            "sender_name": doc.get("sender_name", []),
            "recipient_email": doc.get("recipient_email", []),
            "recipient_name": doc.get("recipient_name", []),
            # "estimated_creation_date": doc.get("estimated_creation_date", [])

        }

        print(f'\n====\nProcessing file: {file_path}')
        print(metadata_map[file_path])

        # Build hierarchical structure
        parts = file_path.split("/")
        current_level = hierarchy
        for part in parts:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]

    return hierarchy, metadata_map

def safe_join(items):
    """Safely joins a list of strings with commas, ignoring non-string items."""
    return ", ".join(str(item) for item in items if item)


def generate_html_structure(hierarchy, metadata_map, path="", level=0, skip_levels=5):
    """Generates the HTML structure for the file and folder hierarchy."""
    result = ""

    for name, sub_items in sorted(hierarchy.items()):
        full_path = "/".join([path, name]) if path else name
        metadata = metadata_map.get(full_path, {})
        show_path = full_path.replace("media/henk/LaCie/2025_MODAL", "")

        # Generate content only if we're at or below the desired level
        if level >= skip_levels:

            # Prepare NER lines conditionally
            ner_persons = safe_join(metadata.get('NER_persons', []))
            ner_organisations = safe_join(metadata.get('NER_organisations', []))
            ner_locations = safe_join(metadata.get('NER_locations', []))
            ner_miscellaneous = safe_join(metadata.get('NER_miscellaneous', []))
            topic_representation = safe_join(metadata.get('TOPIC_representation', []))
            sender_email = safe_join(metadata.get('sender_email', []))
            sender_name = safe_join(metadata.get('sender_name', []))
            recipient_email = safe_join(metadata.get('recipient_email', []))
            recipient_name = safe_join(metadata.get('recipient_name', []))
            # estimated_creation_date = safe_join(metadata.get('estimated_creation_date', []))

            ner_info = ""
            if sender_email:
                ner_info += f"<strong>Sender Email:</strong> {sender_email}<br>"
            # if sender_name:
            #     ner_info += f"<strong>Sender Name:</strong> {sender_name}<br>"
            if recipient_email:
                ner_info += f"<strong>Recipient Email:</strong> {recipient_email}<br>"
            # if recipient_name:
            #     ner_info += f"<strong>Recipient Name:</strong> {recipient_name}<br>"
            if ner_persons:
                ner_info += f"<strong>NER Persons:</strong> {ner_persons}<br>"
            if ner_organisations:
                ner_info += f"<strong>NER Organisations:</strong> {ner_organisations}<br>"
            if ner_locations:
                ner_info += f"<strong>NER Locations:</strong> {ner_locations}<br>"
            if ner_miscellaneous:
                ner_info += f"<strong>NER Miscellaneous:</strong> {ner_miscellaneous}<br>"
            if topic_representation:
                ner_info += f"<strong>Topic Representation:</strong> {topic_representation}<br>"
            # if estimated_creation_date:
            #     ner_info += f"<strong>Estimated Creation Date:</strong> {estimated_creation_date}<br>"

            # Then build the summary block
            summary = f"""
                <div class='metadata'>
                    <div><strong>Summary:</strong> {safe_join(metadata.get('summary', []))}</div>
                    <div><strong>Topic Label:</strong> {safe_join(metadata.get('TOPIC_label', []))}</div>
                    <div><italic>Path:</italic> {html.escape(show_path)}</div>
                    <div class='ner-info'>
                        {ner_info}
                    </div>
                </div>
            """ if metadata else "<div class='metadata'>No summary available</div>"

            if sub_items:
                result += "<ul>"
                result += f'<li><span class="folder" onclick="toggleFolder(this)">{name}</span>'
                result += summary
                result += f'<div class="nested">{generate_html_structure(sub_items, metadata_map, full_path, level + 1, skip_levels)}</div></li>'
                result += "</ul>"
            else:
                result += "<ul>"
                result += f'<li><span class="file">{name}</span>'
                result += summary
                result += '</li></ul>'

        else:
            # Skip rendering this level, but keep traversing its children
            result += generate_html_structure(sub_items, metadata_map, full_path, level + 1, skip_levels)

    return result



def generate_html():
    """Generates and saves an HTML file displaying the hierarchical structure."""
    hierarchy, metadata_map = build_hierarchy()
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>File and Folder Structure {collection_name}</title>
        
        <style>
            body {{ font-family: Arial, sans-serif; }}
            h2 {{background: lightblue; padding: 5px 10px;}}
            ul {{ list-style-type: none; }}
            .folder {{ cursor: pointer; font-weight: bold; color: #007BFF; border: 1px solid grey; background: snow; width: 100%; padding: 5px 10px; font-family: Courier New, monospace;}}
            .nested {{ display: none; margin-left: 30px; padding: 5px 5px;}}
            .metadata {{ font-size: 0.9em; color: #666; margin-left: 30px; position: relative; padding: 5px 5px; border: 1px solid #ccc;}}
            .file {{font-family: Courier New, monospace; font-weight: bold;}}
        
            .ner-info {{
                display: none;
                background-color: #f9f9f9;
                border: 1px solid #ccc;
                opacity: 0.9;
                padding: 8px;
                position: absolute;
                top: 10%;
                left: 3%;
                z-index: 10;
                min-width: 250px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.2);
            }}
        
            .metadata:hover .ner-info {{
                display: block;
            }}
        </style>

        <script>
            function toggleFolder(element) {{
                var nested = element.nextElementSibling.nextElementSibling;
                if (nested.style.display === "block") {{
                    nested.style.display = "none";
                }} else {{
                    nested.style.display = "block";
                }}
            }}
        </script>
    </head>
    <body>
        <h2>MODAL archive browser {collection_name}</h2>
        <p>Klik op de naam van een folder om te openen, hou je muis over metadata voor meer info.</p>
        {generate_html_structure(hierarchy, metadata_map)}
    </body>
    </html>
    """

    html_filename = f"/home/henk/DATABLE/1_Projecten/2024_MODAL/3_Data/browser/{collection_name}_structure.html"
    os.makedirs(os.path.dirname(html_filename), exist_ok=True)
    with open(html_filename, "w", encoding="utf-8") as file:
        file.write(html_content)
    print(f"\n\nHTML file generated: {html_filename}")


# Run the script
generate_html()