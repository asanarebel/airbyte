import datetime
import time

import jwt
import requests

from typing import Any, List, Mapping

class SearchAds:
    
    def __init__(
            self,
            **kwargs
        ):
            """
            Init API instance
            """
            self.__dict__.update(kwargs)
            self.api_version = "v4"
            self.session = None
            self.verbose = False
            self.access_token = None

    def get_access_token_from_client_secret(self, key):
        if self.access_token is None:
            audience = "https://appleid.apple.com"
            alg = "ES256"
            # Define issue timestamp.
            issued_at_timestamp = int(datetime.datetime.utcnow().timestamp())
            # Define expiration timestamp. May not exceed 180 days from issue timestamp.
            expiration_timestamp = issued_at_timestamp + 86400 * 180

            # Define JWT headers.
            headers = dict()
            headers["alg"] = alg
            headers["kid"] = self.key_id

            # Define JWT payload.
            payload = dict()
            payload["sub"] = self.client_id
            payload["aud"] = audience
            payload["iat"] = issued_at_timestamp
            payload["exp"] = expiration_timestamp
            payload["iss"] = self.team_id

            # # Path to signed private key.
            # KEY_FILE = key

            # with open(KEY_FILE, "r") as key_file:
            #     key = "".join(key_file.readlines())

            client_secret = jwt.encode(
                payload=payload, headers=headers, algorithm=alg, key=key
            )
            # with open(f'{KEY_FILE}.txt', 'w') as output:
            #     output.write(client_secret.decode("utf-8"))
            result = requests.post(
                "https://appleid.apple.com/auth/oauth2/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": client_secret,
                    "scope": "searchadsorg",
                },
                headers={
                    "Host": "appleid.apple.com",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )

            # return client_secret
            access_token = result.json()["access_token"]
            # set global access token
            self.access_token = access_token
        else:
            # get global access_token
            access_token = self.access_token
        if self.verbose:
            print(access_token)
        return access_token

class SearchAdsAuthenicator(SearchAds):

    def __init__(self, auth_method: str = "Bearer", auth_header: str = "Authorization", **kwargs):
         super().__init__(**kwargs)
         self.auth_method = auth_method
         self.auth_header = auth_header
         self.access_token = self.get_access_token_from_client_secret(self.key)

    
    def get_auth_header(self) -> Mapping[str, Any]:
        headers = {}
        headers[self.auth_header] = f"{self.auth_method} {self.access_token}"
        headers["X-AP-Context"] = f"orgId={self.org_id}"
        return headers