import json
import os

from dotenv import load_dotenv
from utils import json_serializer
load_dotenv()

API_KEY = os.environ.get('API_KEY')
API_KEY_SECRET = os.environ.get('API_KEY_SECRET')
ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET')

BEARER_TOKEN = os.environ.get('BEARER_TOKEN')

import requests
base_url = "https://api.twitter.com/2/"

from producer import get_producer

myproducer = get_producer()


"""
steps to build a stream
- Create a rule
- Add tag to rule
- Add rule to stream
- authenticate your request
- add rule to stream
- Identify and specify which fields you would like to retrieve
- Connect to the stream and review your response

"""


# rule creation
rule_url_suffix = "tweets/search/stream/rules"
stream_url_suffix = "tweets/search/stream"


def add_rules(value=None, tag=None):
    """
    input
        value: str
        tag: str
    
    output:
        on success: response from twitter api
    """

    sample_rules = [
        {"value": "databases", "tag": "databases pictures"},
        {"value": "plsql has:images", "tag": "plsql pictures"},
    ]

    if value == None:
        payload = {"add": sample_rules}
    else:
        payload = {"add": [{"value": value, "tag": tag if tag != None else ""}]}
    
    # payload = {"add": sample_rules}
    response = requests.post(
        f"{base_url}{rule_url_suffix}", 
        headers= {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {BEARER_TOKEN}'
        }, 
        data=json.dumps(payload)
    )
    if response.status_code != 201:
        raise Exception(
            f"Cannot add rules (HTTP Response Code - {response.status_code}): {response.text}"
        )
    print(json.dumps(response.json()))

def get_rules():
    response = requests.get(
        f"{base_url}{rule_url_suffix}",
        headers= {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {BEARER_TOKEN}'
        },
    )
    if response.status_code != 200:
        raise Exception(
            f"Cannot get rules (HTTP Response Code - {response.status_code}): {response.text}"
        )
    print(json.dumps(response.json()))
    return response.json()

def delete_rules(rule_ids=None):
    """
    input:
        rule_ids -> list of rule ids
        if rule_ids == None, all rules will be deleted

    """
    try:
        if (type(rule_ids) != list) and (rule_ids != None):
            # print("Provide a varaible of type -> List")
            raise Exception(
                f"Invalid data type {type(rule_ids)} provide.\nProvide a varaible of type -> List"
            )
            
        
        if rule_ids == None:
            payload = {
                "delete" : {
                    "ids": [id['id'] for id in get_rules()['data']]
                }
            }
        else:
            payload = {
                "delete": {
                    "ids": rule_ids
                }
            }
    
        response = requests.post(
            f"{base_url}{rule_url_suffix}",
            headers= {
                'Content-type': 'application/json',
                'Authorization': f'Bearer {BEARER_TOKEN}'
            },
            data=json.dumps(payload)
        )
        if response.status_code != 200:
            raise Exception(
                f"Cannot delete rules (HTTP Response Code - {response.status_code}): {response.text}"
            )
        print(json.dumps(response.json()))
        return response.json()
    except Exception as e:
        print(e)
        return e


def start_stream():
    response = requests.get(
        f"{base_url}{stream_url_suffix}",
        headers= {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {BEARER_TOKEN}'
        },
        stream=True
    )
    if response.status_code != 200:
        raise Exception(
            f"Cannot start stream (HTTP Response Code - {response.status_code}): {response.text}"
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            print(json.dumps(json_response, indent=4, sort_keys=True))
            myproducer.produce(
                'python_topic',
                # value=json.dumps(json_response).encode('utf-8')
                value=json_serializer(json_response)
            )




# start_stream()


# # print("invoking delete rule function \n\n\n")
# delete_rules()
# # print("rules deleted")

# print('\n\n\n')
# # get_rules()
# # print (json.dumps(res))

# # print(type(["1568591011470778370"]))
# # import pprint
# # pprint.pprint(json.dumps(res), indent=4)
# # requests.post()


if __name__ == "__main__":
    # pass
    # delete_rules()
    # add_rules()
    add_rules(value="queen")
    # get_rules()
    
    start_stream()
    # print(start_stream())



















# auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# api = tweepy.API(auth=auth)

# if api != None:
#     print("Authenticated successfully")

# class MyStreamListener(tweepy.Stream):

#     def on_status(self, status):
#         print(status.text)

    # def on_data(self, data):
    #     """
    #     input:
    #         data: JSON format
    #     """
    #     # send data through socket
    #     try:
    #         # 
    #         message = json.loads( data )
    #         print(message)

    #     except Exception as e:
    #         pass
    #         #
    #     return True

    # def on_error(self, status_code):
    #     if status_code == 420:
    #         return False
        

# class MyStreamClient(tweepy.StreamingClient):
#     def on_data(self, data):
#         print (json.loads( data))

# class MyStreamRule(tweepy.StreamRule):


# if __name__ == "__main__":
    # stream = MyStreamListener(API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # stream.filter(track=['databases'])

    # stream = MyStreamClient(bearer_token=BEARER_TOKEN, return_type=dict)
    # stream.add_rules(tweepy.StreamRule('databases'))
    # stream.filter()

