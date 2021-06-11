# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import base64
from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import Enum
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import urlparse, parse_qsl

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol import SyncMode


class TwilioStream(HttpStream, ABC):
    url_base = "https://api.twilio.com/2010-04-01/"
    primary_key = "sid"
    page_size = 100

    @property
    def data_field(self):
        return self.name

    def path(self, **kwargs):
        return f"{self.name.title()}.json"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        stream_data = response.json()
        next_page_uri = stream_data.get("next_page_uri")
        if next_page_uri:
            next_url = urlparse(next_page_uri)
            next_page_params = dict(parse_qsl(next_url.query))
            return next_page_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        records = response.json().get(self.data_field, [])
        yield from records

    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(next_page_token=next_page_token, **kwargs)
        params["PageSize"] = self.page_size
        if next_page_token:
            params.update(**next_page_token)
        return params


class TwilioNestedStream(TwilioStream):
    url_base = "https://api.twilio.com"
    media_exist_validation = {}

    def path(self, stream_slice: Mapping[str, Any], **kwargs):
        return stream_slice['subresource_uri']

    @property
    def subresource_uri_key(self):
        return self.data_field

    @property
    @abstractmethod
    def parent_stream(self) -> TwilioStream:
        """
        :return: parent stream class
        """

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        stream_instance = self.parent_stream(authenticator=self.authenticator)
        stream_slices = stream_instance.stream_slices(sync_mode=SyncMode.full_refresh, cursor_field=stream_instance.cursor_field)
        for stream_slice in stream_slices:
            for item in stream_instance.read_records(
                    sync_mode=SyncMode.full_refresh, stream_slice=stream_slice, cursor_field=stream_instance.cursor_field):
                if item.get('subresource_uris', {}).get(self.subresource_uri_key):
                    validated = True
                    for key, value in self.media_exist_validation.items():
                        validated = item.get(key) and item.get(key) != value
                        if not validated:
                            break
                    if validated:
                        yield {'subresource_uri': item['subresource_uris'][self.subresource_uri_key]}


class Accounts(TwilioStream):
    pass


class Addresses(TwilioNestedStream):
    parent_stream = Accounts


class DependentPhoneNumbers(TwilioNestedStream):
    parent_stream = Addresses
    url_base = "https://api.twilio.com/2010-04-01/"

    def path(self, stream_slice: Mapping[str, Any], **kwargs):
        return f"Accounts/{stream_slice['account_sid']}/Addresses/{stream_slice['sid']}/DependentPhoneNumbers.json"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        stream_instance = self.parent_stream(authenticator=self.authenticator)
        stream_slices = stream_instance.stream_slices(sync_mode=SyncMode.full_refresh, cursor_field=stream_instance.cursor_field)
        for stream_slice in stream_slices:
            for item in stream_instance.read_records(
                    sync_mode=SyncMode.full_refresh, stream_slice=stream_slice, cursor_field=stream_instance.cursor_field):
                yield {'sid': item['sid'], 'account_sid': item['account_sid']}


class Applications(TwilioNestedStream):
    parent_stream = Accounts


class AvailablePhoneNumberCountries(TwilioNestedStream):
    parent_stream = Accounts
    data_field = "countries"
    subresource_uri_key = 'available_phone_numbers'


class AvailablePhoneNumbersLocal(TwilioNestedStream):
    parent_stream = AvailablePhoneNumberCountries
    data_field = "available_phone_numbers"
    subresource_uri_key = 'local'


class AvailablePhoneNumbersMobile(TwilioNestedStream):
    parent_stream = AvailablePhoneNumberCountries
    data_field = "available_phone_numbers"
    subresource_uri_key = 'mobile'


class AvailablePhoneNumbersTollFree(TwilioNestedStream):
    parent_stream = AvailablePhoneNumberCountries
    data_field = "available_phone_numbers"
    subresource_uri_key = 'toll_free'


class IncomingPhoneNumbers(TwilioNestedStream):
    parent_stream = Accounts


class Keys(TwilioNestedStream):
    parent_stream = Accounts


class Calls(TwilioNestedStream):
    parent_stream = Accounts


class Conferences(TwilioNestedStream):
    parent_stream = Accounts


class ConferenceParticipants(TwilioNestedStream):
    parent_stream = Conferences


class OutgoingCallerIds(TwilioNestedStream):
    parent_stream = Accounts


class Recordings(TwilioNestedStream):
    parent_stream = Accounts


class Transcriptions(TwilioNestedStream):
    parent_stream = Accounts


class Queues(TwilioNestedStream):
    parent_stream = Accounts


class Messages(TwilioNestedStream):
    parent_stream = Accounts


class MessageMedia(TwilioNestedStream):
    parent_stream = Messages
    data_field = 'media'
    media_exist_validation = {"num_media": "0"}


class UsageRecords(TwilioNestedStream):
    parent_stream = Accounts


class UsageTriggers(TwilioNestedStream):
    parent_stream = Accounts


class Alerts(TwilioStream):
    url_base = "https://monitor.twilio.com/v1/"


class Customers(TwilioStream):
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
class IncrementalTwilioStream(TwilioStream, ABC):
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


class Employees(IncrementalTwilioStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


class HttpBasicAuthenticator(TokenAuthenticator):
    def __init__(self, auth: Tuple[str, str], auth_method: str = "Basic", **kwargs):
        auth_string = f"{auth[0]}:{auth[1]}".encode("utf8")
        b64_encoded = base64.b64encode(auth_string).decode("utf8")
        super().__init__(token=b64_encoded, auth_method=auth_method, **kwargs)


class SourceTwilio(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = HttpBasicAuthenticator((config["account_sid"], config["auth_token"],), auth_method="Basic")

        streams = [
            Accounts(authenticator=auth),
            Addresses(authenticator=auth),
            DependentPhoneNumbers(authenticator=auth),
            Applications(authenticator=auth),
            AvailablePhoneNumberCountries(authenticator=auth),
            AvailablePhoneNumbersLocal(authenticator=auth),
            AvailablePhoneNumbersMobile(authenticator=auth),
            AvailablePhoneNumbersTollFree(authenticator=auth),
            IncomingPhoneNumbers(authenticator=auth),
            Keys(authenticator=auth),
            Calls(authenticator=auth),
            Conferences(authenticator=auth),
            ConferenceParticipants(authenticator=auth),
            OutgoingCallerIds(authenticator=auth),
            Recordings(authenticator=auth),
            Transcriptions(authenticator=auth),
            Queues(authenticator=auth),
            Messages(authenticator=auth),
            MessageMedia(authenticator=auth),
            UsageRecords(authenticator=auth),
            UsageTriggers(authenticator=auth),
            Alerts(authenticator=auth),
        ]
        return streams


# ['addresses', 'conferences', 'signing_keys', 'transcriptions',
# 'connect_apps', 'sip', 'authorized_connect_apps',
# 'usage', 'keys', 'applications', 'recordings',
# 'short_codes', 'calls', 'notifications',
# 'incoming_phone_numbers', 'queues', 'messages',
# 'outgoing_caller_ids', 'available_phone_numbers', 'balance']