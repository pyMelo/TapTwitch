import requests
import json
from dotenv import load_dotenv
import os

#topgames_url = 'https://api.twitch.tv/helix/games/top'

load_dotenv()


client_id_key= os.getenv("client_id")
client_id_secret= os.getenv("client_secret")

streams_url = 'https://api.twitch.tv/helix/streams?first=100'
    


def write_token_to_file(token):
    with open("token.txt", "w") as file:
        file.write(token)

def read_token_from_file():
    with open("token.txt", "r") as file:
        return file.read()
    
def auth_token():
    auth_url = 'https://id.twitch.tv/oauth2/token?'
    auth_param = {
        'client_id': 'client_id_key',
        'client_secret': 'client_id_secret',
        'grant_type': 'authorization_code',
        'code': '',
        'redirect_uri': 'http://localhost:3000',
        'scope' : '',   
    }

    auth_response = requests.post(auth_url,params=auth_param)
    response_data = (json.loads(auth_response.content.decode()))
    token = response_data['access_token']
    expires_date = response_data['expires_in']
    expires_minutes = expires_date // 60
    print("The token.. : " + token)
    print("The token expires in.. "  + expires_minutes + " minutes ")
    write_token_to_file(token)

def taking_datas():

    token = read_token_from_file()
    twitch_headers = {
        'Authorization':'Bearer '+ token,
        'Client-ID': 'client_id_key'
    }

    stream_id = 0
    print('Sending data...')
    response = requests.get(streams_url, headers=twitch_headers)
    stream_data = json.loads(response.content)



    for stream in stream_data['data']:
            

            user_id = stream['user_id']
            user_info_url = f'https://api.twitch.tv/helix/users?id={user_id}'
            response2 = requests.get(user_info_url, headers=twitch_headers)
            user_data = json.loads(response2.content)
            

            follows_url = f'https://api.twitch.tv/helix/users/follows?to_id={user_id}'
            response3 = requests.get(follows_url, headers=twitch_headers)
            follows_data = json.loads(response3.content)

            del stream['id']
            del user_data['data'][0]['id']
        

            data = {
                'id': stream_id,
                'twitch-data': {
                    'stream':{
                        'user_id': stream ['user_id'],
                        'user_name': stream['user_name'],
                        'game_id': stream['game_id'],
                        'game_name': stream['game_name'],
                        'viewer_count': stream['viewer_count'],
                        'language' : stream['language'],

                    },
                    'user':{
                        'profile_image_url': user_data['data'][0]['profile_image_url'],
                        'created_at': user_data['data'][0]['created_at']
                    }, 
                    'follower_count': follows_data['total'],
                }
            }
            data_json = json.dumps(data)


            headers_log = {'Content-type': 'application/json'}
            fluent_url = 'http://localhost:9090'
            requests.post(fluent_url, data=data_json, headers=headers_log)


            stream_id += 1
    
    print('Data sent successfully.')

def main():
     while True:
        user_input = input("Code been taken? (y/n): ")
        if user_input.lower() == "y":
            print("Sending the api's")
            taking_datas()

        if user_input.lower() == 'n':
            print("Making the auth and then sending api's")
            auth_token()
            taking_datas()

main()