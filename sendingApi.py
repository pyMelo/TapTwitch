import requests
import json
import time
from dotenv import load_dotenv
from igdb_api_python import igdb
import os
import csv
import datetime

load_dotenv()
auth_url = "https://id.twitch.tv/oauth2/token?"


def write_data_to_csv(file_path, data):
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data)



client_id_key = os.getenv("client_id")
client_id_secret = os.getenv("client_secret")
code = os.getenv("code")

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


def main():

    totalAPI = 0
    streams_url = "https://api.twitch.tv/helix/streams?first=100"


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
    logstash_counter = 0

    # CICLE TO STREAM ALL THE DATAS ////////////////////////////////////////////////
    for stream in twitch_data["data"]:
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


            genres = []  # Initialize genres as an empty list
            total_rating = None 
            if isinstance(games_data, list) and len(games_data) > 0:
                for game_data in games_data:
                    if game_data.get("name") and game_data.get("name").lower() == game_name.lower():
                        aggregated_rating = game_data.get("aggregated_rating")
                        total_rating = game_data.get("total_rating")
                        release_date = game_data.get("first_release_date")
                        if release_date is not None:
                            release_datetime = datetime.datetime.fromtimestamp(release_date)
                            release_date_formatted = release_datetime.strftime("%Y-%m-%d")
                            release_date = release_date_formatted
                        genres = game_data.get("genres")
                        if not genres:
                            continue
                        genre_names = [genre.get("name") for genre in genres]
                        break
            if not total_rating:
                continue



            
                        # Assuming you have the following variables defined:
            # stream_id, stream, total_rating for the streamer information
            print(f"ID: {stream_id} |")
            print(f"Streamer name: {stream['user_name']} |")
            print(f"Current viewers: {stream['viewer_count']} |")
            print(f"Streamer game: {stream['game_name']} |")
            print(f"Total_Rating: {total_rating} |")
            print("------------------")

            # IGBD REQUESTS API ////////////////////////////////////////////////

            timestamp = time.time()
            datetime_object = datetime.datetime.fromtimestamp(timestamp)
            formatted_datetime = datetime_object.strftime("%Y-%m-%d %H:%M:%S")
            formatted_datetime = formatted_datetime

            data = {
                "id": stream_id,
                "timestamp": formatted_datetime,
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


    #         data_row = [
    #             formatted_datetime,
    #             stream['user_id'],
    #             stream['user_name'],
    #             stream['language'],
    #             stream['game_name'],
    #             stream['viewer_count'],
    #             total_rating,
    #             release_date,
    #             ', '.join(genre_names) if genre_names else ""
    # ]
            data_json = json.dumps(data)

            headers_log = {"Content-type": "application/json"}
            fluent_url = "http://localhost:9090"
            end_response = requests.post(fluent_url, data=data_json, headers=headers_log)



            stream_id += 1

            if stream_id == 25:
                break

    print( str(stream_id) + " Data sent successfully.")
        

for i in range(10):
    main()
    time.sleep(900)  # Sleep for 15 minutes (900 seconds)