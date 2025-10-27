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

#define dag
@dag(
    dag_id="practice",
    start_date=datetime(2025, 10, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["test"],
)

def sandbox(weather):
    """
    This dag will retreive messages, read them, delete them, then assemble a message
    """
    #Task/function to get attributes
    @task(retries=3, retry_delay=timedelta(seconds=10))
    def namer(name):
        #graceful error handling
        try:
            print(f"My name is {name}")
        except Exception as e:
            #print error message
            print(f"Error getting queue attributes: {e}")
            raise e

#Execution
if __name__ == "__main__":
    #Run the dag with the desired url to collect messages and url to send to
    sandbox("Ryan")