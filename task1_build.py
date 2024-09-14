import pymongo.errors
from pymongo import MongoClient  # Import MongoClient for database connection
import json
import sys
import time

# Function to insert messages into the collection
def insert_messages(collection, file_name):
    """Task1 Step1: Insert the messages with batches of 5k messages at a time into the collection"""
    
    # Open the JSON file containing messages
    with open(file_name, "r") as file:
        batch_size = 5000 # Define the batch size
        batch = []
        line_number = 0  # Track the line number for error reporting

        # Iterate through each line in the file
        for line in file:
            line = line.strip()
            line_number += 1
            
            # Skip the first and last lines in the file
            if line in ['[', ']']: 
                continue
            
            # Remove the trailing comma if present
            if line.endswith(','): 
                line = line[:-1]
            
            try:
                # Load the modified line as JSON
                message = json.loads(line)
            except Exception as e:
                # Handle JSON parsing errors
                print(f"Error loading JSON data at line {line_number}: {e}")
                return False
            
            # Add the message to the batch
            batch.append(message)

            # Insert the batch into the collection when batch is full
            if len(batch) == batch_size:
                try:
                    collection.insert_many(batch)
                except pymongo.errors.PyMongoError as e:
                    # Handle MongoDB insertion errors
                    print(f"Error inserting batch: {e}")
                    return False
                batch = []  # Reset the batch

        # Insert any remaining messages in the last batch
        if batch:
            try:
                collection.insert_many(batch)
            except pymongo.errors.PyMongoError as e:
                # Handle MongoDB insertion errors
                print(f"Error inserting batch: {e}")
                return False

    return True

# Function to insert senders into the collection
def insert_senders(collection, file_name):
    """Task1 Step2: Insert the senders directly into the collection"""
    
    # Open the JSON file containing senders
    with open(file_name, 'r') as file:
        try:
            senders_data = json.load(file)
            collection.insert_many(senders_data)
        except Exception as e:
            # Handle JSON parsing or MongoDB insertion errors
            print(f"Error inserting senders: {e}")
            return False

    return True

# Function to check the inserted data in the collection
def check_inserted_data(database_name, collection_name, limit=10):
    """Function to check the data in the collection after insertion"""
    
    # Retrieve a cursor with limited documents from the specified collection
    cursor = database_name[collection_name].find().limit(limit)

    # Print the data in the collection
    print("Checking data in", collection_name, ":")
    for document in cursor:
        print(document)

# Function to build the database and insert messages and senders
def task1_build(port_number):
    try: # connect to MongoDB Server
        client = MongoClient(f'mongodb://localhost:{port_number}/')
        #print("Connected successfully!")
        db = client["MP2Norm"] # Select the database
        #print("Client created!")

        try:
            collection_names = db.list_collection_names() # List existing collection names
        except pymongo.errors.OperationFailure as e:
            # Handle MongoDB operation errors
            print(f"Failed to list collection names: {e}")
            sys.exit(1)

        # Task 1 Step 1: Dropping existing 'messages' collection if it exists
        if "messages" in collection_names: # Check if 'messages' collection exists
            #print("Existing 'messages' collection found.")
            try:
                db.drop_collection("messages") # Drop existing 'messages' collection
                #print("Dropped existing 'messages' collection.")
            except pymongo.errors.OperationFailure as e:
                # Handle MongoDB operation errors
                print(f"Failed to drop 'messages' collection: {e}")

        # create collection and insert messages data
        start_time = time.time() # Record the start time
        #print("Inserting messages...")
        messages_collection = db["messages"] # Create 'messages' collection
        messages_flag = insert_messages(messages_collection, "messages.json") # Insert messages into the collection
        end_time = time.time() # Record the end time
        time_taken = end_time - start_time # Calculate the time taken

        if messages_flag:
            #print("Inserted messages successfully!")
            print(f"Time taken to insert messages: {time_taken:.2f} seconds")
        else:
            print("Error inserting messages")
            sys.exit(1)

        # Task 1 Step 2: Dropping existing 'senders' collection if it exists
        if "senders" in collection_names:
            #print("Existing 'senders' collection found.")
            try:
                db.drop_collection("senders")
                #print("Dropped existing 'senders' collection.")
            except pymongo.errors.OperationFailure as e:
                # Handle MongoDB operation errors
                print(f"Failed to drop 'senders' collection: {e}")

        # insert senders data
        start_time = time.time()
        #print("Inserting senders...")
        senders_collection = db["senders"]
        senders_flag = insert_senders(senders_collection, "senders.json")
        end_time = time.time()
        time_taken = end_time - start_time

        if senders_flag:
            #print("Inserted senders successfully!")
            print(f"Time taken to insert senders: {time_taken:.2f} seconds")
            #print("*******************************************\n")
        else:
            print("Error inserting senders")
            sys.exit(1)

        # check the data in the collections
        #check_inserted_data(db, "messages")
        #check_inserted_data(db, "senders")

        return db, messages_collection, senders_collection

    except pymongo.errors.ConnectionFailure:
        # Handle MongoDB connection errors
        print("Could not connect to MongoDB")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2: # Check if the port number is provided
        print("Usage: python3 task1_build.py <port_number>")
        sys.exit(1)

    port_number = sys.argv[1] # Get the port number from command line arguments

    try:
        port_number = int(port_number)
    except ValueError:
        # Handle invalid port number
        print("Invalid port number. Port number must be an integer.")
        sys.exit(1)

    db, _, _ = task1_build(port_number)
    db.client.close()  # Close the database connection
