import requests
import time
import random
from typing import Dict, Any

import requests
import time
import random
from typing import Dict, Any, List

class RequestHandler:
    """Handles HTTP requests with specified headers and proxy support for web scraping purposes."""

    def __init__(self, headers: Dict[str, str], sleep_secs_min: int, sleep_secs_max: int, retries_max: int, proxies: List[str]):
        """
        Initializes the RequestHandler with specific headers, rate limiting parameters, and proxies.
        
        :param headers: A dictionary of headers to be used in HTTP requests.
        :param sleep_secs_min: Minimum number of seconds to sleep between requests.
        :param sleep_secs_max: Maximum number of seconds to sleep between requests.
        :param retries_max: Maximum number of retry attempts on a failed request.
        :param proxies: A list of proxies to rotate between for making requests.
        """
        self.headers = headers
        self.sleep_secs_min = sleep_secs_min
        self.sleep_secs_max = sleep_secs_max
        self.retries_max = retries_max
        self.proxies = proxies
        self.session = requests.Session()
        self.session.headers.update(headers)

    def sleep(self):
        """
        Sleeps for a random amount of time between sleep_secs_min and sleep_secs_max, inclusive.
        """
        time.sleep(random.randint(self.sleep_secs_min, self.sleep_secs_max))

    def rotate_proxy(self) -> Dict[str, str]:
        """
        Rotates and selects a random proxy from the list for use in the next request.
        
        :return: A dictionary with the http proxy settings.
        """
        if self.proxies:
            proxy_url = random.choice(self.proxies)
            return {
                "http": proxy_url,
                "https": proxy_url
            }
        return {}

    def construct_url(self, url_template: str, api_endpoint_structure: Dict[str, Any]) -> str:
        """
        Constructs a URL from a template and a dictionary describing the API endpoint structure.
        
        :param url_template: A string template for the URL which contains placeholders.
        :param api_endpoint_structure: A dictionary containing the parameters to format the URL template.
        :return: A formatted URL string.
        """
        return url_template.format(**api_endpoint_structure)

    def request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Makes HTTP requests with retries, exponential backoff, and proxy rotation.
        
        :param method: HTTP method to use ('get' or 'post').
        :param url: URL to which the request is sent.
        :param kwargs: Additional arguments to pass to requests methods.
        :return: Response object from requests.
        """
        attempt = 0
        while attempt < self.retries_max:
            try:
                proxy = self.rotate_proxy()
                response = self.session.request(method, url, proxies=proxy, **kwargs)
                response.raise_for_status()
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
        Sends a GET request to the specified URL with retries and proxy rotation.
        
        :param url: URL to which the GET request is sent.
        :return: Response object from requests.
        """
        self.sleep()  # Sleep before making the GET request to manage request rate
        return self.request_with_retry('get', url)

    def post(self, url: str, payload: Dict[str, Any]) -> requests.Response:
        """
        Sends a POST request with a payload to the specified URL with retries and proxy rotation.
        
        :param url: URL to which the POST request is sent.
        :param payload: A dictionary of data to send in the body of the POST request.
        :return: Response object from requests.
        """
        self.sleep()  # Sleep before making the POST request to manage request rate
        return self.request_with_retry('post', url, json=payload)
