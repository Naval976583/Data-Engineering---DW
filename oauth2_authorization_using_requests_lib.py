import sys
import requests
import json
import logging
import time
from urllib.parse import urlencode

logging.captureWarnings(True)

test_api_url = "https://github.com/Naval976583?tab=projects"  # Update the API endpoint as needed


# function to obtain a new OAuth 2.0 token from the authentication server
def get_new_token():
    auth_server_url = "https://github.com/login/oauth/authorize"
    token_url = "https://github.com/login/oauth/access_token"
    client_id = 'e013d79703ce62d4d159'
    client_secret = '42a49d34290ebff54dd6d407f8e0afc9013cd1cd'
    redirect_uri = 'https://github.com/Naval976583'  # Set the redirect URI as per your GitHub OAuth App settings

    # Construct the authorization URL
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': 'user',  # Adjust the scope as needed
        'state': 'your_state'  # Optional but recommended for security
    }
    auth_url = auth_server_url + '?' + urlencode(params)

    print("Please visit the following URL and authorize the application:")
    print(auth_url)
    authorization_code = input("Enter the authorization code: ")

    # Exchange the authorization code for an access token
    token_req_payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'code': authorization_code,
        'redirect_uri': redirect_uri,
        'state': 'your_state'  # Optional but recommended for security
    }

    token_response = requests.post(token_url, data=token_req_payload, verify=False)
    if token_response.status_code != 200:
        print("Failed to obtain token from the OAuth 2.0 server", file=sys.stderr)
        sys.exit(1)

    print("Successfully obtained a new token")
    # tokens = json.loads(token_response.text)
    access_token = token_response.text.split("=")[1]
    return access_token


# 	obtain a token before calling the API for the first time

token = get_new_token()
cnt = 3
while True:
    if cnt == 0:
        print("Could Not Get Valid Token")
        break
    api_call_headers = {'Authorization': 'Bearer ' + token}
    api_call_response = requests.get(test_api_url, headers=api_call_headers)
    if api_call_response.status_code == 401:
        token = get_new_token()
        cnt -= 1
    else:
        print(api_call_response.text)
        print("Successfully Executed")
        sys.exit(0)

    time.sleep(30)
