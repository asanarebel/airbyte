#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

#connector specific imports
from .authenticator import SearchAds, SearchAdsAuthenicator
import hashlib
from datetime import datetime,timedelta


"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class AppleSearchAdsStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class AppleSearchAdsStream(HttpStream, ABC)` which is the current class
    `class Customers(AppleSearchAdsStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(AppleSearchAdsStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalAppleSearchAdsStream((AppleSearchAdsStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "https://api.searchads.apple.com/api/v4/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}


class Customers(AppleSearchAdsStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "customer_id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "customers"


# Basic incremental stream
class IncrementalAppleSearchAdsStream(AppleSearchAdsStream, ABC):

    def __init__(self, limit: str, **kwargs):
        super().__init__(**kwargs)
        self.limit = limit

    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}

class KeywordsReport(IncrementalAppleSearchAdsStream):

    http_method = "POST"
    cursor_field = "startTime"
    limit = 1000

    # in cases that the campaign doesn't have keywords we'll receive a 400 error code
    # then we have to prevent the exception 
    @property
    def raise_on_http_errors(self) -> bool:
        return False

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["startTime","keywordId"]

    def __init__(self,start_date="",**kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.limit = 1000

    def path(self,stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        
        return f"reports/campaigns/{stream_slice['campaignId']}/keywords"

    def get_campaigns(self):

        auth = self.authenticator.get_auth_header()
        url = f'{self.url_base}campaigns'
        res = requests.get(url,headers=auth).json()['data']
        
        for campaign in res:
            yield {'campaignId':campaign['id']}

    def stream_slices(self, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:

        campaigns = self.get_campaigns()
        i=0
        for campaign in campaigns:
            i+=1

        if stream_state.get(self.cursor_field) is not None:
            self.start_date = stream_state.get(self.cursor_field)

        start = datetime.strptime(self.start_date,'%Y-%m-%d')
        end = datetime.today()
        daterange = [start + timedelta(days=x) for x in range(0, (end-start).days)]

        for campaign in campaigns:
            for _date in daterange:
                next_date = _date+timedelta(days=1)
                yield {'startTime': _date.strftime('%Y-%m-%d'),'endTime': next_date.strftime('%Y-%m-%d'),'campaignId': campaign['campaignId']}

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:

        latest = max(current_stream_state.get(self.cursor_field, ""), latest_record.get(self.cursor_field, ""))

        return {self.cursor_field:latest}


    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:

        params = dict()
        params = {'selector': 
                    {'orderBy': 
                        [{'field': 'keywordId',
                            'sortOrder': 'ASCENDING' }
                        ],
                    'conditions': [],
                    'pagination': {'offset': 0,
                    'limit': self.limit }},
                'timeZone': 'UTC',
                'returnRecordsWithNoMetrics': False,
                'returnRowTotals': False,
                'returnGrandTotals': False,
                'granularity': 'DAILY'
                }

        if next_page_token:
            params.update(next_page_token)

        params.update(stream_slice)
        del params['campaignId']
            
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        response_dict = []

        non_keywords_error_messages = ['APPSTORE_SEARCH_TAB CAMPAIGN DOES NOT CONTAIN KEYWORD',
                            'APPSTORE_PRODUCT_PAGES_BROWSE CAMPAIGN DOES NOT CONTAIN KEYWORD']

        # in case there are no keywords for the campaign, we'll receive a 400 error with a specific message
        if response.status_code == 400:
            for error in response.json()['error']['errors']:
                if error['message'] in non_keywords_error_messages:
                    return response_dict

        response_json = response.json()["data"]["reportingDataResponse"]

        for row in response_json["row"]:
            
            # add create a row for each row in the granularity
            for granularity in row['granularity']:
                granularity_row = dict()
                # bring the date (startTime) and keywordId to the first json level
                # granularity_row['primary_key'] = hashlib.sha1(str(granularity['date']+
                #                     str(row['metadata']['keywordId'])).encode('utf-8')).hexdigest()
                granularity_row['metadata'] = row['metadata']
                granularity_row['keywordId'] = row['metadata']['keywordId']
                granularity_row['startTime'] = granularity['date']
                granularity_row['metrics'] = granularity
                granularity_row['insights'] = row['insights']
                response_dict.append(granularity_row)
        yield from response_dict

class SearchTermsReport(IncrementalAppleSearchAdsStream):

    http_method = "POST"
    cursor_field = "startTime"
    limit = 1000

    def __init__(self,start_date="",**kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.limit = 10000

    # in cases that the campaign doesn't have keywords we'll receive a 400 error code
    # then we have to prevent the exception 
    @property
    def raise_on_http_errors(self) -> bool:
        return False

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["adGroupId","startTime","keywordId","searchTermText"]

    def path(self,stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        
        return f"reports/campaigns/{stream_slice['campaignId']}/searchterms"

    # get all existing campaigns and yield them to the stream
    def get_campaigns(self):

        limit = 100
        offset = 0
        total_results = 0

        auth = self.authenticator.get_auth_header()

        while total_results >= offset:
            url = f'{self.url_base}campaigns'
            res = requests.get(url,headers=auth,params={"limit":limit,"offset":offset}).json()

            total_results = int(res['pagination']['totalResults'])
            offset+=limit

            print(offset)

            for campaign in res['data']:
                yield {'campaignId':campaign['id']}

    def stream_slices(self, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:

        campaigns = self.get_campaigns()

        if stream_state.get(self.cursor_field) is not None:
            self.start_date = stream_state.get(self.cursor_field)

        start = datetime.strptime(self.start_date,'%Y-%m-%d')
        end = datetime.today()
        daterange = [start + timedelta(days=x) for x in range(0, (end-start).days)]

        for campaign in campaigns:
            for _date in daterange:
                #next_date = _date+timedelta(days=1)
                formated_date = _date.strftime('%Y-%m-%d')
                #yield {'startTime': _date.strftime('%Y-%m-%d'),'endTime': next_date.strftime('%Y-%m-%d'),'campaignId': campaign['campaignId']}
                yield {'startTime': formated_date,'endTime': formated_date,'campaignId': campaign['campaignId']}

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:

        latest = max(current_stream_state.get(self.cursor_field, ""), latest_record.get(self.cursor_field, ""))

        return {self.cursor_field:latest}


    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:

        params = dict()
        params = {'selector': 
                    {'orderBy': 
                        [{'field': 'adGroupId',
                    'sortOrder': 'ASCENDING' }],
                    'conditions': [],
                    'pagination': {'offset': 0,
                    'limit': 1000 }},
                'timeZone': 'UTC',
                'returnRecordsWithNoMetrics': False,
                'returnRowTotals': False,
                'returnGrandTotals': False,
                'granularity': 'DAILY'
                }

        if next_page_token:
            params.update(next_page_token)

        params.update(stream_slice)
        del params['campaignId']
            
        return params

    def parse_response(self, response: requests.Response,stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:

        response_dict = []

        non_keywords_error_messages = ['APPSTORE_SEARCH_TAB CAMPAIGN DOES NOT CONTAIN SEARCHTERM',
                            'APPSTORE_PRODUCT_PAGES_BROWSE CAMPAIGN DOES NOT CONTAIN SEARCHTERM']

        # in case there are no keywords for the campaign, we'll receive a 400 error with a specific message
        if response.status_code == 400:
            for error in response.json()['error']['errors']:
                if error['message'] in non_keywords_error_messages:
                    return response_dict

        try:
            response_json = response.json()["data"]["reportingDataResponse"]
        except TypeError as e:
            return response_dict

        for row in response_json["row"]:
            
            # add create a row for each row in the granularity
            for granularity in row['granularity']:
                granularity_row = dict()
                granularity_row['metadata'] = row['metadata']
                granularity_row['startTime'] = granularity['date']
                granularity_row['adGroupId'] = row['metadata']['adGroupId']
                granularity_row['keywordId'] = row['metadata']['keywordId']
                granularity_row['searchTermText'] = row['metadata']['searchTermText']
                granularity_row['campaignId'] = stream_slice['campaignId']
                granularity_row['metrics'] = granularity
                yield granularity_row
                
"""
API Documentation
https://developer.apple.com/documentation/apple_search_ads/get_campaign-level_reports
"""
class CampaignsReport(IncrementalAppleSearchAdsStream):
    
    http_method = "POST"
    cursor_field = "startTime"
    limit = 1000
    start = 0

    def __init__(self,start_date="",**kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.limit = 1000

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["startTime","campaignId","adamId","countryOrRegion"]

    def path(self, **kwargs) -> str:
        
        return "reports/campaigns"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        response_dict = list()
        response_json = response.json()["data"]["reportingDataResponse"]

        for row in response_json["row"]:
            
            for granularity in row['granularity']:
                # print('test_campaign:',row['metadata']['campaignId'],"date:",granularity['date'],'adamId:',row['metadata']['app']['adamId'],'countryOrRegion:',row['metadata']['countryOrRegion'])
                granularity_row = dict()
                granularity_row['campaignId'] = row['metadata']['campaignId']
                granularity_row['startTime'] = granularity['date']
                granularity_row['adamId'] = row['metadata']['app']['adamId']
                granularity_row['countryOrRegion'] = row['metadata']['countryOrRegion']
                # granularity_row['primary_key'] = hashlib.md5(str(granularity['date']+
                #                                 str(row['metadata']['campaignId'])).encode('utf-8')+
                #                                 str(row['metadata']['app']['adamId']).encode('utf-8')+
                #                                 str(row['metadata']['countryOrRegion']).encode('utf-8')
                #                                 ).hexdigest()
                granularity_row['metadata'] = self.flatteningJSON(row['metadata'])
                granularity_row['metrics'] = self.flatteningJSON(granularity)

                response_dict.append(granularity_row)

        return response_dict

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        
        response_data = response.json()
        self.total_records = int(response_data["pagination"]["totalResults"])
        #res_len = len(response_data["data"]["reportingDataResponse"]["row"])
        if self.start+self.limit >= response_data["pagination"]["totalResults"]:
            return None
        else:
            self.start+=self.limit
            return {"pagination": {"offset": self.start}}

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:

        params = dict()
        params = {
                'selector': {'orderBy': 
                                [
                                    {'field': 'startTime',
                                        'sortOrder': 'ASCENDING'
                                    }
                                ],
                'conditions': [],
                'pagination': {'offset': self.start,
                                'limit': self.limit
                                }},
                'timeZone': 'UTC',
                'returnRecordsWithNoMetrics': False,
                'returnRowTotals': False,
                'returnGrandTotals': False,
                'granularity': 'DAILY',
                'groupBy': ['countryOrRegion']}

        if next_page_token:
            params.update(next_page_token)

        params.update(stream_slice)
            
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:

        latest = max(current_stream_state.get(self.cursor_field, ""), latest_record.get(self.cursor_field, ""))

        return {self.cursor_field:latest}
    
    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        
        if stream_state.get(self.cursor_field) is not None:
            self.start_date = stream_state.get(self.cursor_field)

        start = datetime.strptime(self.start_date,'%Y-%m-%d')
        end = datetime.today()
        daterange = [start + timedelta(days=x) for x in range(0, (end-start).days)]

        for _date in daterange:
            start_time = _date.strftime('%Y-%m-%d')
            daily_range = {'startTime': start_time,'endTime': start_time}

            yield daily_range

    def flatteningJSON(self,b): 
        ans = {} 
        def flat(i, na =''):
            #nested key-value pair: dict type
            if type(i) is dict: 
                for a in i: 
                    flat(i[a], na + a + '_')
            #nested key-value pair: list type
            # elif type(i) is list: 
            #     j = 0  
            #     for a in i:                 
            #         flat(a, na + str(j) + '_') 
            #         j += 1
            else: 
                ans[na[:-1]] = i 
        flat(b) 
        return ans

class Campaigns(AppleSearchAdsStream):
    
    http_method = "POST"
    #cursor_field = "startTime"
    primary_key = "campaignId"
    start = 0
    limit = 100

    def __init__(self,start_date="",**kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.limit = 100

    def path(self, **kwargs) -> str:
        
        return "campaigns/find"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        response_json = response.json()["data"]
        for row in response_json:
            yield row

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        
        response_data = response.json()
        self.total_records = int(response_data["pagination"]["totalResults"])
        if self.start+self.limit >= self.total_records:
            return None
        else:
            self.start+=self.limit
            return {"pagination": {"offset": self.start}}

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:

        params = dict()
        params = {
            "fields": None,
            "conditions": [
                {
                "field": "deleted",
                "operator": "IN",
                "values": [
                    "true",
                    "false"
                ]
                }
            ],
            "orderBy": [
                {
                "field": "name",
                "sortOrder": "ASCENDING"
                }
            ],
            "pagination": {
                "offset": self.start,
                "limit": self.limit
            }
            }

        if next_page_token:
            params.update(next_page_token)

            
        return params

class Apps(AppleSearchAdsStream):
    
    #http_method = "POST"
    #cursor_field = "startTime"
    primary_key = "adamId"
    start = 0
    limit = 1

    def __init__(self,start_date="",**kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.limit = 1

    def path(self, **kwargs) -> str:
        
        return "search/apps?query=Asana%20Rebel"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        response_json = response.json()["data"]
        for row in response_json:
            yield row

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        
        response_data = response.json()
        self.total_records = int(response_data["pagination"]["totalResults"])
        if self.start+self.limit >= self.total_records:
            return None
        else:
            self.start+=self.limit
            return {"pagination": {"offset": self.start}}



# Source
class SourceAppleSearchAds(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:

        searchads = SearchAds(**config)

        access_token = searchads.get_access_token_from_client_secret(key=config['key'])

        if access_token:
            return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        

        args = {"limit": 1000}
        auth = SearchAdsAuthenicator(**config)
        return [CampaignsReport(authenticator=auth,start_date=config['start_date'],**args),
                Campaigns(authenticator=auth,start_date=config['start_date']), 
                Apps(authenticator=auth,start_date=config['start_date']),
                SearchTermsReport(authenticator=auth,start_date=config['start_date'],**args),
         ]
