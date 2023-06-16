import requests
import json
import time
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


def main():
    while True:

            user_input = input("Retrieve tokens? (y/n): ")

            if user_input.lower() == "y":
                print("Making the auth and then sending api's")
                auth_token()
                client_auth()
                break;
            if user_input.lower() == "n":
                print("Stopping the script.. ")
                break;

main()
