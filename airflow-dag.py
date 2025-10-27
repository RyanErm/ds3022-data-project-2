#Ryan Ermovick - jph4dg

#Questions
#How do I structure the return outputs
#Is the Dag decorator set up properly???
#Do I just place all the run statements inside the dag
#Do I still need the __main__ checker???
#What is the :str thing and the ->str thing
#can I define the queue and stuff before the dag or should I do it below?

#big thing to add, the return outputs!!!!

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import boto3

#set up some variables to work with
queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/jph4dg" 
sqs = boto3.client('sqs')

#define dag
@dag(
    dag_id="message_assembler",
    start_date=datetime(2025, 10, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["project"],
)

def message_assembler_dag(q_url: str, num_messages: int, sender_url: str):
    """
    This dag will retreive messages, read them, delete them, then assemble a message
    """
    #Task/function to get attributes
    @task(retries=3, retry_delay=timedelta(seconds=10))
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

    #Task/function to obtain a message
    #45 retries for the SQS function that searches for 20 seconds each time
    #45*20=900 seconds, which is the longest that a message could be delayed for
    @task(retries=45, retry_delay=timedelta(seconds=10))
    def get_message(queue_url: str) -> str:
        # graceful error handling
        try:
            # try to get any messages with message-attributes from SQS queue:
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
            #return the receipt handle to delete message, order, and word
            return receipt_handle, int(order), word
        except Exception as e:
            #print error message
            print(f"Error getting message: {e}")
            raise e

    #Task/function to delete the message
    @task(retries=3, retry_delay=timedelta(seconds=10))
    def delete_message(queue_url: str, receipt_handle: str):
        #graceful error handling
        try:
            #delete message based off of receipt handle
            response = sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            #Print response
            print("Deleted Message")
            print(f"Response: {response}")
        except Exception as e:
            #print error message
            print(f"Error deleting message: {e}")
            raise e

    #Task/function to sort the dictionary  into the phrase
    @task(retries=3, retry_delay=timedelta(seconds=10))
    def sort_dict(dict):
        #graceful error handling
        try:
            #obtain all the word positions
            keys = list(dict.keys())
            #sort all the positions
            keys.sort()
            #create a new string for the phrase
            full_string = ""
            #loop through each sorted position to find the word that corresponds to it
            for num in keys:
                #Add Spaces when it is not the last word of the phrase, or else there will be an extra space
                if num < (len(keys)-1):
                    new_word = dict[num] + " "
                else:
                    new_word = dict[num]
                full_string += new_word
            #print full string
            print(f"Phrase has been sorted/assembled: {full_string}")
            #return full string
            return full_string
        except Exception as e:
            #print error message
            print(f"Error sending message: {e}")
            raise e
        
    #Task/function to send the final solution
    @task(retries=3, retry_delay=timedelta(seconds=10))
    def send_solution(queue_url, message, uvaid, phrase, platform): 
        #graceful error handling 
        try:
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
        except Exception as e:
            #print error message
            print(f"Error sending message: {e}")
            raise e
        
    #Create a dictionary  to store the unsorted phrase
    my_dict = {}
    #Send all the messages 
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/jph4dg"
    payload = requests.post(url).json #does this need to be the json method??
    print("21 Messages have been sent...")
    #Collect messages until the desired amount come through 
    while (len(my_dict)<num_messages):
        #Get queue attributes
        get_queue_attributes(q_url)
        print("Flow is working - Got queue attributes")
        #Collect the new variables
        receipt, order, word = get_message(q_url)
        print("Flow is working - got a message!")
        #Delete message
        delete_message(q_url, receipt)
        print("Flow is working - Deleted message!")
        #Add the word and order to the dictionary 
        my_dict[order] = word
        print(f"Added the word: {word} with the order number: {order} ")
        print(f"Here is the current unsorted dictionary  of phrases: {my_dict}")
    #Create a new string for the sorted phrase
    phrase = sort_dict(my_dict)
    #Print new phrase
    print(f"The mystery phrase is '{phrase}'")
    #Send solution
    send_solution(sender_url,"Message", "jph4dg", phrase, "airflow")
    print("Flow is working - Final phrase and message sent!")

#Execution

    #Run the dag with the desired url to collect messages and url to send to
project_dag = message_assembler_dag(queue_url, 21, "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit")