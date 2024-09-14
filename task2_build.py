import pymongo.errors
from pymongo import MongoClient
import json
import sys # Import sys for command line argument handling
import time # Import time for performance measurement

def insert_messages_with_senderInfo(messages_collection, file_name, senders_data):
    """
    Insert messages with embedded sender information into the messages collection.
    This version uses a dictionary for senders_data for improved lookup speed.
    """
    with open(file_name, "r") as file:
        batch_size = 10000  # Define the batch size for bulk insertion
        batch = []  # Temporary list to hold messages before inserting in batch
        line_number = 0  # Keep track of line numbers for error reporting

        for line in file:
            line_number += 1  # Increment line number for each line processed
            line = line.strip()
            if line in ['[', ']']:  # Skip JSON array boundaries
                continue
            if line.endswith(','):  # Remove trailing comma for valid JSON
                line = line[:-1]

            try:
                message = json.loads(line)  # Parse the JSON data
            except Exception as e:
                print(f"Error loading JSON data at line {line_number}: {e}")
                return False

            # Look up sender info based on sender_id and embed it into the message
            sender_id = message.get("sender") # Extract sender ID from the message
            sender_info = senders_data.get(sender_id, None)  # Use the dictionary for faster lookup
            if sender_info:
                message["sender_info"] = sender_info  # Embed sender info into the message

            batch.append(message)  # Add the modified message to the batch

            # Insert the batch when it reaches the specified size
            if len(batch) == batch_size: # Check if batch has reached the specified size
                try:
                    messages_collection.insert_many(batch)
                    batch = []  # Reset batch after insertion
                except pymongo.errors.PyMongoError as e:
                    print(f"Error inserting batch at line {line_number}: {e}")
                    return False

        # Insert any remaining messages in the batch
        if batch:
            try:
                messages_collection.insert_many(batch)
            except pymongo.errors.PyMongoError as e:
                print(f"Error inserting the final batch at line {line_number}: {e}")
                return False

    return True

def task2_build(port_number):
    """
    Main function to embed the sender information into the messages collection,
    insert the modified messages into MongoDB, and create necessary indexes.
    """
    try:
        port_number = int(port_number)  # Ensure port number is an integer
    except ValueError:
        print("Invalid port number")
        sys.exit(1)

    client = MongoClient(f'mongodb://localhost:{port_number}/')
    db = client['MP2Embd']  # Connect to the database

    # Drop the 'messages' collection if it already exists to start fresh
    if "messages" in db.list_collection_names():
        db.drop_collection("messages")

    messages_collection = db["messages"]  # Create or select the 'messages' collection

    # Load sender data from JSON file and convert it into a dictionary
    with open("senders.json", "r") as senders_file:
        senders_list = json.load(senders_file)
        senders_data = {item["sender_id"]: item for item in senders_list}

    # Insert messages with embedded sender info and measure the time taken
    start_time = time.time()
    if insert_messages_with_senderInfo(messages_collection, "messages.json", senders_data):
        time_taken = time.time() - start_time
        print(f"Time taken to insert messages with embedded sender info: {time_taken:.2f} seconds")
    else:
        print("Error inserting messages with embedded sender info")
        sys.exit(1) # Exit the program

    return db, messages_collection

if __name__ == "__main__":
    if len(sys.argv) < 2: # Check if port number is provided
        print("Usage: python3 your_script_name.py <port_number>") # Print usage instruction
        sys.exit(1)
    
    port_number = sys.argv[1] # Get port number from command line argument
    db, messages_collection = task2_build(port_number)
    db.client.close() # Close the database connection
