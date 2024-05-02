import sys
import requests
import json
import logging
import time
from urllib.parse import urlencode
import convert_json_to_parquet

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


# obtain a token before calling the API for the first time


# api_url -> put the url of api in this key
# location_of_table_to_store -> put the file path of table to store here
api_to_table_mapping = {
    '{base_url}/v2/CARCDP/CDP_Tags/?$skip={n}': 'location_of_table_to_store'
}

token = get_new_token()
while True:
    api_call_headers = {'Authorization': 'Bearer ' + token, 'Accept': '*/*', 'Accept-Encoding': 'gzip,deflate,br',
                        'Connection': 'keep-alive',
                        'SPFConfigUID': 'PL_Canmore',
                        'SPFIgnoreConfig': 'true', 'SPFIgnoreEffectivity': 'true', 'Prefer': 'odata.maxpagesize=10'
                        }
    i = 1
    for api_url in api_to_table_mapping:
        skip_limit = 500  # set this limit as per your requirement
        base_url = ''  # replace with actual base_url
        for n in range(100, skip_limit + 1, 100):
            api_call_response = requests.get(api_url.format(base_url, n),
                                             headers=api_call_headers)  # replace test_api_url with api and store api_call_response as json in api key value location
            with open(f"data{i}.json", "w") as f:
                json.dump(api_call_response, f)
            if api_call_response.status_code == 401:
                token = get_new_token()
                print("Invalid Token")
                sys.exit(1)
            else:
                convert_json_to_parquet.convert_json_to_parquet()
                print("Successfully Executed")

        i += 1
        time.sleep(30)
