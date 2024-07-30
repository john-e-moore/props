import requests
import time
import random
from typing import Dict, Any, List, Optional

class RequestHandler:
    def __init__(self, headers: dict, sleep_secs_min: int, sleep_secs_max: int, retries_max: int, proxies: Optional[List[str]] = None, max_requests_per_proxy: int = 10):
        """
        Initializes the RequestHandler with optional proxy rotation.
        
        :param headers: A dictionary of headers to be used in HTTP requests.
        :param sleep_secs_min: Minimum number of seconds to sleep between requests.
        :param sleep_secs_max: Maximum number of seconds to sleep between requests.
        :param retries_max: Maximum number of retry attempts on a failed request.
        :param proxies: Optional list of proxies for rotation.
        :param max_requests_per_proxy: Maximum number of requests before rotating proxies.
        """
        self.headers = headers
        self.sleep_secs_min = sleep_secs_min
        self.sleep_secs_max = sleep_secs_max
        self.retries_max = retries_max
        self.request_count = 0
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.proxies = proxies
        self.max_requests_per_proxy = max_requests_per_proxy
        
        if proxies:
            self.current_proxy = random.choice(proxies)
            self.session.proxies.update({"http": self.current_proxy, "https": self.current_proxy})
        else:
            self.current_proxy = None

    def sleep(self):
        """
        Sleeps for a random amount of time between sleep_secs_min and sleep_secs_max, inclusive.
        """
        sleep_secs = random.randint(self.sleep_secs_min, self.sleep_secs_max)
        print(f"Sleeping for {sleep_secs} seconds.")
        time.sleep(sleep_secs)

    def rotate_proxy(self) -> Dict[str, str]:
        """
        Rotates and selects a random proxy from the list for use in the next request.

        Tip: 5-10 requests from the same proxy mimics human behavior. 
        Note: switching proxies does not require a new requests.Session()
        
        :return: A dictionary with the http proxy settings.
        """
        if self.proxies and self.request_count >= self.max_requests_per_proxy:
            self.current_proxy = random.choice(self.proxies)
            self.session.proxies.update({"http": self.current_proxy, "https": self.current_proxy})
            self.request_count = 0  # Reset the count
        elif self.proxies:
            self.request_count += 1

    def construct_url(self, url_template: str, **kwargs) -> str:
        """
        Constructs a URL from a template and a dictionary describing the API endpoint structure.
        
        :param url_template: A string template for the URL which contains placeholders.
        :param kwargs: A dictionary containing the parameters to format the URL template.
        :return: A formatted URL string.
        """
        return url_template.format(**kwargs)

    def request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Makes HTTP requests with retries, exponential backoff, and proxy rotation.
        
        :param method: HTTP method to use ('get' or 'post').
        :param url: URL to which the request is sent.
        :param kwargs: Additional arguments to pass to requests methods.
        :return: Response object from requests.
        """
        # Request parameters
        params = {}
        params['headers'] = self.headers
        if self.proxies:
            params['proxies'] = self.current_proxy
        
        print("**********REQUEST FORMAT**********")
        print(f"Method: {method.upper()}")
        print(f"URL: {url}")
        print(f"Params: {params}")
        
        # Attempt request
        attempt = 0
        while attempt < self.retries_max:
            try:
                response = self.session.request(
                    method=method.upper(), 
                    url=url, 
                    **params
                )
                print(f"Status code: {response.status_code}")
                response.raise_for_status()
                print("Success.")
                self.sleep()
                return response
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                sleep_secs = (2 ** attempt) * random.uniform(1, 2) # Exponential backoff with jitter
                print(f"Retrying in {sleep_secs} seconds.")
                time.sleep(sleep_secs)
                attempt += 1
        print(f"All retry attempts failed after {self.retries_max} attempts.")
        return None

    def get(self, url: str) -> requests.Response:
        """
        Sends a GET request to the specified URL with optional proxy rotation.
        
        :param url: URL to which the GET request is sent.
        :return: Response object from requests.
        """
        
        return self.request_with_retry('get', url)

    def post(self, url: str, payload: Dict[str, Any]) -> requests.Response:
        """
        Sends a POST request with a payload to the specified URL with retries and proxy rotation.
        
        :param url: URL to which the POST request is sent.
        :param payload: A dictionary of data to send in the body of the POST request.
        :return: Response object from requests.
        """

        return self.request_with_retry('post', url, json=payload)
