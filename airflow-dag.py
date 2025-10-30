#Ryan Ermovick - jph4dg
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import boto3
import duckdb

#set up some variables to work with
queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/jph4dg" 
sqs = boto3.client('sqs', region_name = 'us-east-1')
#define dag
@dag(
    dag_id="message_assembler",
    start_date=datetime(2025, 10, 25),
    schedule=None,
    catchup=False,
    tags=["project"],
)

#create the dag
def message_assembler_dag(q_url: str, sender_url: str):
    """
    This dag will retreive messages, read them, delete them, then assemble a message
    """
    #this task sets up the duck db data base so it is ready to go for all other tasks
    @task
    def set_up():
        #graceful error handling
        try:
            #connecting
            con = duckdb.connect(database='/tmp/messages.duckdb', read_only=False)
            print("CONNECTED")
            #make sure the table has the proper columns and data types
            con.execute(""" 
                DROP TABLE IF EXISTS message_data;
                CREATE TABLE message_data(number BIGINT, word TEXT);
            """)
            print("Duck DB table was created successfully")
        except Exception as e:
            #print error message
            print(f"Error sending message: {e}")
            raise e
        
    #this task sends out all messages to be received
    @task
    def initial_messages():
        try:
            url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/jph4dg"
            #Send all the messages 
            payload = requests.post(url).json 
            print("21 Messages have been sent...")
        except Exception as e:
            #print error message
            print(f"Error sending message: {e}")
            raise e
    #this task gets the queue attributes to see how many messages are present
    @task
    def get_queue_attributes(queue_url):
        #graceful error handling
        try:
            #obtain all of the attributes from the queue and print
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
                )
            print("Got Queue Attributes")
            print(f"Response: {response}")
        except Exception as e:
            #print error message
            print(f"Error getting queue attributes: {e}")
            raise e

    #this task finds the message, parses it, and adds it to the duckdb database
    #it has 45 retries for a search period of 20 seconds in case any of the messages are delayed 900 seconds
    @task(retries=45, retry_delay=timedelta(seconds=5))
    def get_messages(queue_url: str):
        # graceful error handling
        try:
            #connect to the duck db database
            con = duckdb.connect(database='/tmp/messages.duckdb', read_only=False)
            print("CONNECTED")
            #obtain the original number of messages
            num_messages = con.execute("SELECT COUNT(*) FROM message_data").fetchone()[0] 
            #loop through the message retrival untill 21 messages are present
            while num_messages<21:
                #get the message
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MessageSystemAttributeNames=['All'],
                    MaxNumberOfMessages=1,
                    VisibilityTimeout=60,
                    MessageAttributeNames=['All'],
                    WaitTimeSeconds=20 #search for 20 seconds!
                )
                #Obtain receipt handle
                receipt_handle = response['Messages'][0]['ReceiptHandle']
                print(f"Got Receipt Handle: {receipt_handle}")
                #Obtain order number
                order = response['Messages'][0]['MessageAttributes']['order_no']['StringValue']
                print(f"Got Order Number: {order}")
                #Obtain word 
                word = response['Messages'][0]['MessageAttributes']['word']['StringValue']
                print(f"Got Word: {word}")
                #add the word into the duckdb database
                con.execute("INSERT INTO message_data VALUES (?, ?)", [int(order), word])
                #delete the message
                response = sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
                #Print response
                print("Deleted Message")
                print(f"Response: {response}")
                #update the num_messages variable
                num_messages = con.execute("SELECT COUNT(*) FROM message_data").fetchone()[0] 
                print(f'There are this many messages collected: {num_messages}')
            #nothing to return
            return None
        except Exception as e:
            #print error message
            print(f"Error sending message: {e}")
            raise e

    #Task/function to send the final solution
    @task
    def send_solution(queue_url, message, uvaid, platform): 
        #graceful error handling 
        try:
            #connect to duckdb
            con = duckdb.connect(database='/tmp/messages.duckdb', read_only=False)
            print("CONNECTED")
            #retrieve the words in a sorted order
            bad_phrase = con.execute("SELECT word FROM message_data ORDER BY number ASC").fetchall()
            #set up an empty string
            phrase = ''
            #loop through the list to get all the strings
            print("Assembling the phrase")
            for i in range(len(bad_phrase)):
                #get the word
                word = bad_phrase[i][0]
                #add a space
                spaced_word = word + ' '
                #add the word to the sentence
                phrase += spaced_word
            phrase = phrase[:-1] #get rid of the last space so it is a normal sentence
            print(f"Here is the phrase: {phrase}")
            #send message with specifically uvaid, phrase, and platform used
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=message,
                MessageAttributes={
                    'uvaid': {
                        'DataType': 'String',
                        'StringValue': uvaid
                    },
                    'phrase': {
                        'DataType': 'String',
                        'StringValue': phrase
                    },
                    'platform': {
                        'DataType': 'String',
                        'StringValue': platform
                    }
                }
            )
            #print response
            print("Message has been sent")
            print(f"Response: {response}")
            print("Flow is working - Final phrase and message sent!")        
        except Exception as e:
            #print error message
            print(f"Error sending message: {e}")
            raise e
    
    #call each task 
    set = set_up()
    start = initial_messages()
    gq1 = get_queue_attributes(q_url)
    messages = get_messages(q_url)
    send= send_solution(sender_url,"Message", "jph4dg", "airflow")
    gq2 = get_queue_attributes(q_url)
    #set up the sequence for tasks
    set >> start >> gq1 >> messages >> send >> gq2

#Execution
#Run the dag with the desired url to collect messages and url to send to
message_assembler_dag(queue_url, "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit")