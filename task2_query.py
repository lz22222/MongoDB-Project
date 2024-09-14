import sys
import pymongo.errors
from pymongo import MongoClient
import time

class task_2_query:
    def __init__(self, port_number):
        #self.db = db
        #self.messages_collection = messages_collection
        client = MongoClient(f'mongodb://localhost:{port_number}/') # Connect to MongoDB using the provided port number
        self.db = client['MP2Embd']  # Connect to the database
        self.messages_collection = self.db["messages"]  # Access the 'messages' collection within the database


    """Task 2 Step 3 begins here"""
    def query_q1(self):
        # Q1: Return the number of messages that have “ant” in their text.
        start_time = time.time()
        try:
            query = {"text": {"$regex": "ant"}} # Define the query to search for "ant" in the message text
            count = self.messages_collection.count_documents(query, maxTimeMS=120000) # Execute the query with a maximum execution time
            print(f"Q1 Result: {count} messages have 'ant' in their text.")
        except pymongo.errors.ExecutionTimeout: # Catch execution timeout exceptions
            print("Step 3 Query 1 took more than 2 minutes to execute.")
        end_time = time.time()
        time_taken_seconds = end_time - start_time  # Calculate the total time taken in seconds
        time_taken_milliseconds = time_taken_seconds * 1000
        print(f"Time taken for Q1: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")

    def query_q2(self):
        # Q2: Find the nickname/phone number of the sender who has sent the greatest number of messages.

        start_time = time.time()
        try:
            query2 = [
                {"$group": {"_id": "$sender", "count": {"$sum": 1}}}, # Group messages by sender and count them
                {"$sort": {"count": -1}}, # Sort the results in descending order by count
                {"$limit": 1} # Limit the results to the top sender
            ]
            result2 = list(self.messages_collection.aggregate(query2, maxTimeMS=120000)) # Execute the aggregation pipeline with a maximum execution time
            if result2:
                sender = result2[0]["_id"] # Extract the sender ID from the results
                count2 = result2[0]["count"]  # Extract the count of messages sent by the top sender
                print(f"Q2 Result: {sender} has sent the greatest number of messages of {count2}.")
                #print(f"Total messages sent: {count2}")
            else:
                print("Q2 Result: No messages found.")
        except pymongo.errors.ExecutionTimeout:
            print("Step 3 Query 2 took more than 2 minutes to execute.")
        end_time = time.time()
        time_taken_seconds = end_time - start_time  # Calculate the total time taken in seconds
        time_taken_milliseconds = time_taken_seconds * 1000
        print(f"Time taken for Q2: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")

    def query_q3(self):
        # Q3: Return the number of messages where the sender’s credit is 0.
        start_time = time.time()
        try:
            query = {"sender_info.credit": 0}  # Define the query to find messages where the sender's credit is 0
            count = self.messages_collection.count_documents(query, maxTimeMS=120000)
            print(f"Q3 Result: {count} messages have senders with credit 0.")
        except pymongo.errors.ExecutionTimeout: # Catch execution timeout exceptions
            print("Step 3 Query 3 took more than 2 minutes to execute.")
        end_time = time.time()
        time_taken_seconds = end_time - start_time # Calculate the total time taken in seconds
        time_taken_milliseconds = time_taken_seconds * 1000
        print(f"Time taken for Q3: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")

    def query_q4(self):
        # Q4: Double the credit of all senders whose credit is less than 100.
        start_time = time.time()
        try:
            query = {"sender_info.credit": {"$lt": 100}}  # Define the query to match senders with credit less than 100
            update = {"$mul": {"sender_info.credit": 2}} # Define the update operation to double the credit
            self.messages_collection.update_many(query, update) # Execute the update operation
        except pymongo.errors.ExecutionTimeout:
            print("Step 3 Query 4 took more than 2 minutes to execute.")
        end_time = time.time()
        time_taken_seconds = end_time - start_time # Calculate the total time taken in seconds
        time_taken_milliseconds = time_taken_seconds * 1000
        print(f"Time taken for Q4: {time_taken_seconds:.2f} seconds ({time_taken_milliseconds:.2f} milliseconds)")

    def run_queries(self):
        self.query_q1()
        self.query_q2()
        self.query_q3()
        self.query_q4()

    def check_credit_before_and_after_q4(self):
        """helper function to check sender's credit before and after running query_q4."""
        sender_id_to_track = "+13863373325"

        # Get the sender's credit before running query_q4
        try:
            sender_before = self.messages_collection.find_one({"sender_info.sender_id": sender_id_to_track})
            credit_before = sender_before["sender_info"]["credit"]
            print(f"Sender {sender_id_to_track} has credit {credit_before} before running query_q4.")
        except (TypeError, KeyError):
            print(f"Sender {sender_id_to_track} not found before running query_q4.")

        self.query_q4()

        # Get the sender's credit after running query_q4
        try:
            sender_after = self.messages_collection.find_one({"sender_info.sender_id": sender_id_to_track})
            credit_after = sender_after["sender_info"]["credit"]
            print(f"Sender {sender_id_to_track} has credit {credit_after} after running query_q4.")
        except (TypeError, KeyError):
            print(f"Sender {sender_id_to_track} not found after running query_q4.")

    def shutdown(self):
        self.db.client.close() # Close the MongoDB client connection

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 your_script_name.py <port_number>")
        sys.exit(1) # Exit the program with an error status

    port_number = sys.argv[1] # Get the port number from the command line arguments

    #db, messages_collection = task2_build(port_number)
    task2_query = task_2_query(port_number) # Instantiate the query class with the provided port number
    task2_query.run_queries()
    task2_query.shutdown()  # Shut down the MongoDB client
