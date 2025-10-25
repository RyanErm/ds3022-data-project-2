# DS3022 Data Project 2

This data project is a puzzle. Your goal is to put the pieces back together in the right order.

Your assignment is to write a Prefect data pipeline that retrieves a number of messages from an SQS queue, parses their contents, and reassembles a complete phrase made up of the fragments from each single message.

Your pipeline can run a single time to produce a complete result, or it can run on a schedule to produce a complete result. Design your workflow however you see fit.

You can run your pipeline as many times as you like for testing. There is no penalty for running it multiple times.

To get started, [**fork this repository**](https://github.com/uvasds-systems/ds3022-data-project-2/fork) and save your work to your own repository.

## Task 1 - Populate your SQS Queue

To populate your SQS queue with messages, you must make a request of an API. Using your UVA computing ID, append it to this API endpoint:

```
https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/<UVA_ID>
```

Your pipeline must call this URL by way of an HTTP `POST` request. This is possible using the `requests` or `httpx` libraries in python:

```
import requests

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mst3k"

payload = requests.post(url).json
```

```
import httpx

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mst3k"

payload = httpx.post(url).json
```

In either case the `payload` object returns your SQS URL (as a reminder if you need it):

```
>>> payload
{'hello': 'mst3k', 'sqs_url': 'https://sqs.us-east-1.amazonaws.com/440848399208/mst3k'}
```

Your request to this API will send exactly **21** messages to your SQS queue. These have been sent with a variety of random `DelaySeconds` values ranging from 30 to 900 seconds.

**Keep these delays in mind as your pipeline proceeds to the next task.**

**NOTE** This step (sending a `POST` request to the API) should not be repeated if your pipeline needs to run more than once, i.e. on a cron timer, as it gathers all messages. The API request clears your queue of all previous messages and repopulates all 21 messages each time.

## Task 2 - Monitor Your Queue then Collect Messages

Next, devise a way for your pipeline to track how many messages are available for pickup using the `get_queue_attributes()` method. As it gets attributes about your queue, notice that there are three values that count messages:

- `ApproximateNumberOfMessages`
- `ApproximateNumberOfMessagesNotVisible`
- `ApproximateNumberOfMessagesDelayed`

Together, these make up the total count of messages in your queue.

You should determine a strategy for how/when to pick up messages, and code according to that strategy.

When receiving messages, each has a `MessageBody` that contains the same word (i.e. meaningless content). The meaningful content of each message is contained within the `['MessageAttributes']` segment of each message, and that you must parse into each attribute and get out its `['StringValue']`.

To get these values:

```
# order number:
['Messages'][0]['MessageAttributes']['order_no']['StringValue']

# word:
['Messages'][0]['MessageAttributes']['word']['StringValue']
```

Store the values you fetch for each message and keep them paired together. There are a variety of ways to store them.

Some notes:

- Recall that even though the `order_no` value will appear as if it is an integer, Python will see it as a string by default.
- You must also fetch the `ReceiptHandle` for each message and delete it after storing its data.
- Your pipeline must receive, parse, and delete ALL messages completely when run. Do not leave "dangling" messages that have been read but not deleted.
- Take care that when you request a message using the boto3 `receive_message` method that you handle errors gracefully without throwing an error and breaking your pipeline. If there are messages but they are invisible or delayed, and you try to poll for it, the returned response will not match the format of a successful message request.

## Task 3 - Reassemble the Messages and Submit Your Answer via SQS

Using the two fields collected from each message, now reassemble the words into a single phrase by ordering them using the `order_no` value.

Take this example:

```
order_no         word
---------------------------------------
3                brown
2                quick
4                fox.
1                The
```

would result in "This quick brown fox."

There are a variety of ways to sort lists or key-value pairs in both Python and SQL.

You may run and observe your pipeline multiple times if you care to, but in the end your code must fetch and reassemble all messages in order without human intervention. You may not receive messages or sort the fragments manually.

Your completed phrase should now be submitted as a message attribute for a new message that you send to a separate SQS queue, along with your computing ID and the platform (either "prefect" or "airflow") used for your pipeline.

Submit your answer to this queue:
```
https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit
```

To send a message with attributes, use this syntax:

```
def send_solution(uvaid, phrase, platform)
    try:
        response = sqs.send_message(
            QueueUrl=url,
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
        print(f"Response: {response}")

    . . .
```
Be sure that your response returns a `200` HTTP response message, indicating that it has been received.

## Task 4 (Optional) - Rewrite this Pipeline as an Airflow DAG

For additional points, write a second data pipeline compatible with Apache Airflow. This step is not *in place of writing a Prefect flow* but in addition to it.

Be sure that your DAG runs successfully within Airflow when you executed in your AWS EC2 instance. It should produce identical results to your Prefect flow, but the final "platform" message attribute you submit should be set to "airflow".

## Notes / Submission

1. Be sure to fork this repository and commit/push your code back to it for grading.
2. Your Prefect flow should be saved to a file named `prefect.py`.
3. When running your Prefect flow you may use the remote host profile we set up in class `[profiles.uvasds]`, or  `[profiles.local]`. Either is fine.
4. If you attempt to write an Airflow DAG that should be saved to a file named `airflow.py`.
5. Secondary Prefect flows or Airflow DAGs are permissible. That is, one flow may also trigger another flow; one DAG may call another DAG, etc.
6. Your code should log using the built-in logging methods for either Prefect or Airflow. You do not need to use a separate logging package. Do not save or commit log files to your repo.
7. Do not save or commit any data or database files.

## AWS Issues

If you experience permissions errors with AWS and your SQS queue, you have two options:

1. Generate a new Access Key and Secret Access Key in your AWS account and then run `aws configure` in your local terminal. Fresh credentials may be needed to authenticate to your own account.
2. Alternatively, use the credentials I distribute with this assignment in Canvas. These keys have very limited access to only the SQS service, so they are only good for this project. Be sure to save your own credentials to restore after this assignment.

Of course if you experience some unusual error please be in touch with the instructor.

## Reference

- [Working with SQS - Practical Examples](https://github.com/nmagee/learn-sqs)
- [`boto3` - SQS Client Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html)
- [Prefect Reference](https://docs.prefect.io/v3/get-started)
- [Airflow DAG Reference](https://s3.amazonaws.com/uvasds-systems/pdfs/Ultimate-Guide-to-Apache-Airflow-DAGs.pdf)