import requests
import json
from dotenv import load_dotenv
from igdb_api_python import igdb
import os
import csv
import datetime

load_dotenv()


client_id_key = os.getenv("client_id")
client_id_secret = os.getenv("client_secret")
code = os.getenv("code")


def write_data_to_csv(file_path, data):
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data)

auth_url = "https://id.twitch.tv/oauth2/token?"


def write_authtoken_to_file(token):
    with open("auth_token.txt", "w") as file:
        file.write(token)


def read_authtoken_from_file():
    with open("auth_token.txt", "r") as file:
        return file.read()


def write_clientoken_to_file(token):
    with open("client_token.txt", "w") as file:
        file.write(token)


def read_clientoken_from_file():
    with open("client_token.txt", "r") as file:
        return file.read()


# AUTH FOR TWITCH API ////////////////////////////////////////////////
def auth_token():
    auth_param = {
        "client_id": client_id_key,
        "client_secret": client_id_secret,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": "http://localhost:3000",
        "scope": "",
    }

    # TOKEN AUTH ////////////////////////////////////////////////
    auth_response = requests.post(auth_url, params=auth_param)
    auth_data = json.loads(auth_response.content.decode())
    token_auth = auth_data["access_token"]
    expires_date = auth_data["expires_in"]
    expires_minutes = expires_date // 60
    print("The authorization code token.. : " + token_auth)
    print("The token expires in.. " + str(expires_minutes) + " minutes ")

    write_authtoken_to_file(token_auth)


# CLIENT AUTH FOR IGBD API ////////////////////////////////////////////////
def client_auth():
    client_param = {
        "client_id": client_id_key,
        "client_secret": client_id_secret,
        "grant_type": "client_credentials",
        "code": code,
        "redirect_uri": "http://localhost:3000",
        "scope": "",
    }

    # CLIENT AUTH ////////////////////////////////////////////////
    client_response = requests.post(auth_url, params=client_param)
    client_data = json.loads(client_response.content.decode())
    token_client = client_data["access_token"]
    expires_date2 = client_data["expires_in"]
    expires_minutes2 = expires_date2 // 60
    print("The client credentials token is.. " + token_client)
    print("The token expires in.. " + str(expires_minutes2) + " minutes ")

    write_clientoken_to_file(token_client)


def taking_datas():
    
    totalAPI = 0
    streams_url = "https://api.twitch.tv/helix/streams?first=50"
    excluded_game_ids = ["509658", "743", "488191","509667","518203"]


    auth_token = read_authtoken_from_file()
    client_token = read_clientoken_from_file()

    twitch_headers = {
        "Authorization": "Bearer " + auth_token,
        "Client-ID": client_id_key,
    }



    igbd_headers = {
        "Authorization": "Bearer " + client_token,
        "Client-ID": client_id_key,
    }

    stream_id = 0
    print("Sending data...")

    # TWITCH REQUESTS API ////////////////////////////////////////////////
    twitch_response = requests.get(streams_url, headers=twitch_headers)
    twitch_data = json.loads(twitch_response.content)

    # CICLE TO STREAM ALL THE DATAS ////////////////////////////////////////////////
    for stream in twitch_data["data"]:
        if stream["game_id"] not in excluded_game_ids:

        # USER IDENTIFIER TO REFER TO FOR EACH BROADCASTER ///////////////////////////////
            user_id = stream["user_id"]

            # GETTING INFOS LIKE PROFILE IMAGES AND WHEN IT WAS CREATED
            user_info_url = f"https://api.twitch.tv/helix/users?id={user_id}"
            response2 = requests.get(user_info_url, headers=twitch_headers)
            user_data = json.loads(response2.content)

            # GETTING NUMBER OF FOLLOWERS OF THAT USER
            follows_url = f"https://api.twitch.tv/helix/users/follows?to_id={user_id}"
            response3 = requests.get(follows_url, headers=twitch_headers)
            follows_data = json.loads(response3.content)

            del stream["id"]
            del user_data["data"][0]["id"]
            game_name = stream["game_name"]


            igbd_url = "https://api.igdb.com/v4/games/"
            igbd_params = {
                "fields": "name,genres.name,total_rating,first_release_date,aggregated_rating",
                "search": game_name,
                "limit": 50,
            }


            igbd_response = requests.get(igbd_url, headers=igbd_headers,params=igbd_params)
            games_data = json.loads(igbd_response.content)


            if isinstance(games_data, list) and len(games_data) > 0:
                for game_data in games_data:
                    if game_data.get("name").lower() == game_name.lower():
                        aggregated_rating = game_data.get("aggregated_rating")
                        total_rating = game_data.get("total_rating")
                        release_date = game_data.get("first_release_date")
                        if release_date is not None:
                            release_datetime = datetime.datetime.fromtimestamp(release_date)
                            release_date_formatted = release_datetime.strftime("%Y-%m-%d")
                            release_date = release_date_formatted
                            genres = game_data.get("genres")
                        if genres:
                            genre_names = [genre.get("name") for genre in genres]
                        break
                else:
                    aggregated_rating = None
                    total_rating = None
                    release_date = None
                    genres = None



            print("Streamer name  : " + str(stream['user_name']) + " | \nStreamer game : " + str(stream['game_name']) + " | \nTotal_Rating : " + (str(total_rating)) + " | ")
            # IGBD REQUESTS API ////////////////////////////////////////////////

            data = {
                "id": stream_id,
                "twitch-data": {
                    "stream": {
                        "user_id": stream["user_id"],
                        "user_name": stream["user_name"],
                        "game_id": stream["game_id"],
                        "game_name": stream["game_name"],
                        "viewer_count": stream["viewer_count"],
                        "language": stream["language"],
                        "profile_image_url": user_data["data"][0]["profile_image_url"],
                        "created_at": user_data["data"][0]["created_at"],   
                        "follower_count": follows_data["total"],
                    },
                    "aggregated_rating": aggregated_rating,
                    "total_rating": total_rating,
                    "release_date" : release_date,
                    "genres" : genre_names
                },
            }

            data_row = [
                stream['user_name'],
                stream['game_name'],
                stream['viewer_count'],
                total_rating,
                release_date,
                ', '.join(genre_names) if genre_names else ""
    ]
            data_json = json.dumps(data)
            write_data_to_csv("data.csv", data_row)

            headers_log = {"Content-type": "application/json"}
            fluent_url = "http://localhost:9090"
            requests.post(fluent_url, data=data_json, headers=headers_log)
            totalAPI += 1

            stream_id += 1

    print( totalAPI + " Data sent successfully.")


def main():
    while True:
        user_input = input("Code been taken? (y/n): ")
        if user_input.lower() == "y":
            print("Sending the api's")
            taking_datas()

        if user_input.lower() == "n":
            print("Making the auth and then sending api's")
            auth_token()
            client_auth()
            taking_datas()


main()
