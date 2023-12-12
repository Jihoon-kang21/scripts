from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from pprint import pprint

# Establish a connection to MongoDB
client = MongoClient("mongodb://id:password@10.111.123.123:1001,10.111.123.124:1002,10.111.123.125:1003/app")
db = client.get_database()  # Replace with your actual database name

# Function to convert user input to the appropriate value
def convert_input_value(input_value):
    if input_value.startswith("$gte:") and len(input_value) > len("$gte:"):
        date_str = input_value[len("$gte:"):]
        try:
            return {"$gte": datetime.fromisoformat(date_str)}
        except ValueError:
            # Handle invalid date format
            return None
    elif input_value.startswith("$in:"):
        return {"$in": input_value[len("$in:"):].split(',')}
	elif input_value.lower() == "true":
		return True
    elif input_value.lower() == "false":
        return False
    elif ObjectId.is_valid(input_value):
        return ObjectId(input_value)
    elif input_value.isdigit():
        return int(input_value)
    else:
        return input_value

# Function to convert user input to a sorting criteria
def get_sort_criteria():
    sort_key = input("Enter a sorting key (or press Enter to skip): ")
    if sort_key:
        sort_order = int(input("Enter the sorting order (1 for ascending, -1 for descending): "))
        return [(sort_key, sort_order)]
    return None

# Function to get user input for keys to exclude from results
def get_keys_to_exclude():
    keys_to_exclude = input("Enter keys to exclude (comma-separated, or press Enter to skip): ")
    if keys_to_exclude:
        return keys_to_exclude.split(',')
    return None

def get_distinct_values(collection, key):
    distinct_values = collection.distinct(key)
    return distinct_values

def list_collections(db):
	return db.list_collection_names()

available_collections = list_collections(db)
print("Available collections: ")
print(" / ".join(available_collections))
# Get user input for the collection name
collection_name = input("Enter the collection name: ")

# Fetch the keys for the specified collection
sample_doc = db[collection_name].find_one()
collection_keys = list(sample_doc.keys()) if sample_doc else []

# Display the available keys for the specified collection
print(f"Available keys for the collection '{collection_name}':")
print(", ".join(collection_keys))

# Get user input for key-value pairs
query_dict = {}
while True:
    key = input("Enter a key (or press Enter to finish): ")
    if not key:
        break
    value = input("Enter the value: ")

    query_dict[key] = convert_input_value(value)

sort_criteria = get_sort_criteria()
keys_to_exclude = get_keys_to_exclude()
limit_str = input("Enter limit (or press Enter to skip): ")
limit = int(limit_str) if limit_str else None
distinct_option = input("Do you want to retrieve distinct values for a specific key? (yes/no): ")

if distinct_option.lower() == "yes":
    key_to_inspect = input("Enter the key for which you want to see distinct values: ")
    # Query the database using pymongo to get distinct values for the specified key
    collection = db[collection_name]
    distinct_values = get_distinct_values(collection, key_to_inspect)

    # Display the distinct values
    if distinct_values:
        print(f"Distinct values for '{key_to_inspect}':")
        for value in distinct_values:
            print(value)
    else:
        print(f"No distinct values found for '{key_to_inspect}' in the collection '{collection_name}'.")
else:
    print("Distinct values retrieval skipped.")

# Print the entire generated query
print("Generated Query:")
pprint(query_dict)

# Query the database using pymongo (for constructing the query)
collection = db[collection_name]
results = collection.find(query_dict)

# Apply sorting criteria if provided
if sort_criteria:
    results = results.sort(sort_criteria)

total_count = collection.count_documents(query_dict)

# Conditionally limit the results based on user input
if limit is not None:
    results = results.limit(limit)

# Print the results with excluded keys
for result in results:
    result_to_display = result.copy()
    if keys_to_exclude:
        for key in keys_to_exclude:
            result_to_display.pop(key, None)
    pprint(result_to_display)

print(f"Total count of matching documents: {total_count}")

