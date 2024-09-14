import pymongo.errors # Import pymongo errors module for handling database errors
from pymongo import MongoClient
import sys
import time

class task_1_query:
    def __init__(self, port_number):
        # Initialize the database connection using the given port number
        #self.db, self.messages_collection, self.senders_collection = task1_build(port_number)
        client = MongoClient(f'mongodb://localhost:{port_number}/')
        #print("Connected successfully!")
        self.db = client["MP2Norm"]
        self.messages_collection = self.db["messages"] # Access the messages collection
        self.senders_collection = self.db["senders"] # Access the senders collection

    def query_q1(self):
        # Query 1: Count messages containing the substring "ant" in their text.
        # This demonstrates a simple search using regex.
        start_time = time.time()  # Start timing the query
        try:
            query1 = {"text": {"$regex": "ant"}}  # Define the query
            count1 = self.messages_collection.count_documents(query1, maxTimeMS=120000)  # Execute the query
            print(f"Q1 Result: {count1} messages have 'ant' in their text.")
        except pymongo.errors.ExecutionTimeout: # Handle timeout errors
            print("Step 3 Query 1 took more than 2 minutes to execute.")
        end_time = time.time() 
        time_taken_seconds = end_time - start_time # Calculate time taken in seconds
        time_taken_milliseconds = time_taken_seconds * 1000 # Convert time taken to milliseconds
        print(f"Time taken for Q1: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")

    def query_q2(self):
        # Query 2: Find the sender who has sent the most messages.
        # This is achieved by grouping messages by sender, counting them, and sorting in descending order.
        start_time = time.time()  # Start timing the query
        try:
            query2 = [
                {"$group": {"_id": "$sender", "count": {"$sum": 1}}},  # Group by sender and count
                {"$sort": {"count": -1}},  # Sort by count in descending order
                {"$limit": 1}  # Limit to the top result
            ]
            result2 = list(self.messages_collection.aggregate(query2, maxTimeMS=120000))  # Execute the aggregation pipeline with a 2-minute timeout
            if result2:
                sender = result2[0]["_id"] # Extract the sender ID from the result
                count2 = result2[0]["count"]  # Extract the message count from the result
                print(f"Q2 Result: {sender} has sent the greatest number of messages of {count2}.")
                #print(f"Total messages sent: {count2}")
            else:
                print("Q2 Result: No messages found.")
        except pymongo.errors.ExecutionTimeout:
            print("Step 3 Query 2 took more than 2 minutes to execute.")
        end_time = time.time()
        time_taken_seconds = end_time - start_time  # Calculate time taken in seconds
        time_taken_milliseconds = time_taken_seconds * 1000 # Convert time taken to milliseconds
        print(f"Time taken for Q2: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")

    def query_q3(self):
        # Query 3: Count messages from senders with zero credit.
        # This involves a subquery to first find senders with zero credit.
        start_time = time.time()  # Start timing the query
        try:
            query3 = {"sender": {"$in": self.senders_collection.distinct("sender_id", {"credit": 0})}}  # Define the query to match senders with zero credit
            count3 = self.messages_collection.count_documents(query3, maxTimeMS=120000)  # Execute the query with a 2-minute timeout
            print(f"Q3 Result: {count3} messages have senders with credit 0.")
        except pymongo.errors.ExecutionTimeout: # Handle timeout errors
            print("Step 3 Query 3 took more than 2 minutes to execute.")
        end_time = time.time()  # End timing the query execution
        time_taken_seconds = end_time - start_time
        time_taken_milliseconds = time_taken_seconds * 1000
        print(f"Time taken for Q3: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")

    def query_q4(self):
        # Query 4: Double the credit for senders with credit less than 100.
        # This updates the sender's credit using a multiplier update operation.
        start_time = time.time()  # Start timing the query
        try:
            query4 = {"credit": {"$lt": 100}}  # Define the query
            update4 = {"$mul": {"credit": 2}}  # Define the update operation
            self.senders_collection.update_many(query4, update4)  # Execute the update operation
        except pymongo.errors.ExecutionTimeout:
            print("Step 3 Query 4 took more than 2 minutes to execute.")
        end_time = time.time()
        time_taken_seconds = end_time - start_time
        time_taken_milliseconds = time_taken_seconds * 1000
        print(f"Time taken for Q4: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")
        #print("*******************************************\n")



    def senders_with_zero_credit(database_name, collection_name):
        """helper function that prints all the senders with credit equal to 0"""

        cursor = database_name[collection_name].find({"credit": 0})  # Find all senders with zero credit

        print("Senders with credit equal to 0:")
        for document in cursor: # Iterate through the results
            print(document) # Print each sender's document

    def create_indices(self):
        """Task 1 Step 4: create indices for fields 'sender' and 'text' in messages collection,
        and an index for 'sender_id' in senders collection"""

        try:
            self.messages_collection.create_index("sender") # Create index for 'sender'
            self.messages_collection.create_index([("text", "text")]) # Create text index for 'text'
            self.senders_collection.create_index("sender_id") # Create index for 'sender_id'
            #print("Indices created successfully!")
        except pymongo.errors.OperationFailure as e:  # Handle operation failure errors
            print(f"Failed to create index: {e}")

    def shutdown(self):
        """Close the connection to the MongoDB server"""

        self.db.client.close() # Close the MongoDB client connection

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 task1_query.py <port_number>") # Prompt for correct script usage
        sys.exit(1) # Exit with error status

    port_number = sys.argv[1]  # Get the port number from command line arguments

    try:
        port_number = int(port_number) # Convert port number to an integer
    except ValueError: # Handle invalid port number
        # Handle invalid port number
        print("Invalid port number. Port number must be an integer.")
        sys.exit(1)
    
    task1_query = task_1_query(port_number) # Create an instance of the query class
    # Task 1 Step 3:
    task1_query.query_q1()
    task1_query.query_q2()
    task1_query.query_q3()
    task1_query.query_q4()
    # Task 1 Step 4:  Re-execute queries to measure performance improvement
    task1_query.create_indices()
    print('\n')
    task1_query.query_q1()
    task1_query.query_q2()
    task1_query.query_q3()
    #task1_query.query_q4()
    task1_query.shutdown() # Shut down the database connection
