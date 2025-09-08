
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import numpy as np

# CHOOSE SETTINGS
top_n = 50  # Number of top items to display
enrichment_type = "recipient_name"  # Change this to any enrichment type you want to analyze
min_occurrences = 1  # Minimum number of occurrences to include in analysis
item_name = enrichment_type
name_filter = "emmerechts"

# MongoDB connection details
mongo_uri = "mongodb://localhost:27017/"
DB_NAME = "MODAL_testdata"
COLLECTION_NAME = "LH_HH_71_Kristien_Hemmerechts"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Dictionary to store item occurrences by year
item_year_counts = defaultdict(lambda: defaultdict(int))
# Dictionary to store total occurrences per item
item_total_counts = defaultdict(int)
# Dictionary to store total occurrences per year
year_total_counts = defaultdict(int)

# Iterate through documents in the collection
for document in collection.find():
    # Get the year from estimated_creation_date
    year = document.get("estimated_creation_date", "")
    if not year or year == "N/A":
        continue

    # Try to extract year as integer
    try:
        year = int(str(year)[:4])  # Take first 4 characters in case date is in full format
    except (ValueError, TypeError):
        continue

    # Get items from enrichments
    if enrichment_type in document:
        # Check if the field contains an array of items
        # print(type[enrichment_type])
        items = document[enrichment_type]

        # Process each item in the array
        if isinstance(items, list):
            for item in items:
                # Clean and normalize the item string
                item = item.strip()
                print(item)

                if item and name_filter not in item.lower():  # Skip empty strings
                    item_year_counts[item][year] += 1
                    item_total_counts[item] += 1
                    year_total_counts[year] += 1
        # # Handle the case where it's a single string instead of an array
        # elif isinstance(items, str):
        #     item = items.strip()
        #     if item:
        #         item_year_counts[item][year] += 1
        #         item_total_counts[item] += 1
        #         year_total_counts[year] += 1
    else:
        print(f"Enrichment type '{enrichment_type}' not found in document.")

# Filter out items with less than minimum occurrences
filtered_items = {item: years for item, years in item_year_counts.items()
                 if item_total_counts[item] >= min_occurrences}

# Convert to DataFrame
data = []
for item in filtered_items:
    row = {item_name.capitalize(): item}
    row.update(filtered_items[item])
    data.append(row)

df = pd.DataFrame(data)

# Set item as index and sort columns
df.set_index(item_name.capitalize(), inplace=True)
df.sort_index(inplace=True)
df = df.reindex(sorted(df.columns), axis=1)

# Fill NaN values with 0
df = df.fillna(0)

# Convert counts to integers
df = df.astype(int)

# Calculate total occurrences for each item
totals = df.sum(axis=1).sort_values(ascending=False)

# Select top N most frequently mentioned items
top_items = totals.head(top_n).index
df_top = df.loc[top_items]

# Convert absolute numbers to percentages
df_percentages = df_top.copy()
for year in df_percentages.columns:
    if year_total_counts[year] > 0:  # Avoid division by zero
        df_percentages[year] = (df_percentages[year] / year_total_counts[year] * 100)

# Create visualizations
sns.set_theme()

# 1. Line plot (percentages)
plt.figure(figsize=(15, 8))
for item in df_percentages.index:
    plt.plot(df_percentages.columns, df_percentages.loc[item], marker='o', label=item)

plt.title(f'Relative Frequency of Top {top_n} {item_name.title()}s Over Time (% per year)', pad=20)
plt.xlabel('Year')
plt.ylabel('Percentage of Total Occurrences')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()

# Save line plot
line_plot_file = f"/home/henk/DATABLE/1_Projecten/2024_MODAL/3_Data/exports/{COLLECTION_NAME}_{enrichment_type}_line_plot_percentage.png"
plt.savefig(line_plot_file, dpi=300, bbox_inches='tight')

# 2. Heatmap (percentages)
# plt.figure(figsize=(15, 10))
plt.figure(figsize=(15, max(10, len(df_percentages) * 0.3)))  # Dynamic height based on number of rows

sns.heatmap(df_percentages, cmap='YlOrRd', annot=True, fmt='.1f',

            cbar_kws={'label': 'Percentage of Total Occurrences'})
plt.title(f'Heatmap of {item_name.title()} Relative Frequency by Year (%)', pad=20)
plt.xlabel('Year')
plt.ylabel(item_name.capitalize())

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Adjust layout to prevent label cutoff
plt.tight_layout()


# Save heatmap
heatmap_file = f"/home/henk/DATABLE/1_Projecten/2024_MODAL/3_Data/exports/{COLLECTION_NAME}_{enrichment_type}_heatmap_percentage.png"
plt.savefig(heatmap_file, dpi=300, bbox_inches='tight')

# Save both absolute and percentage data to CSV
output_file_abs = f"/home/henk/DATABLE/1_Projecten/2024_MODAL/3_Data/exports/{COLLECTION_NAME}_{enrichment_type}_year_matrix_absolute_min{min_occurrences}.csv"
output_file_pct = f"/home/henk/DATABLE/1_Projecten/2024_MODAL/3_Data/exports/{COLLECTION_NAME}_{enrichment_type}_year_matrix_percentage_min{min_occurrences}.csv"
df.to_csv(output_file_abs)
df_percentages.to_csv(output_file_pct)

print(f"Absolute numbers exported to {output_file_abs}")
print(f"Percentages exported to {output_file_pct}")
print(f"Line plot saved to {line_plot_file}")
print(f"Heatmap saved to {heatmap_file}")

# Print statistics
print(f"\nTotal number of unique {item_name}s: {len(item_total_counts)}")
print(f"Number of {item_name}s with ≥{min_occurrences} occurrences: {len(filtered_items)}")

# Display total occurrences and yearly totals
print(f"\nTotal occurrences for top {top_n} {item_name}s:")
print(totals.head(top_n))
print("\nTotal occurrences per year:")
year_totals = pd.Series(year_total_counts).sort_index()
print(year_totals)