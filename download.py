import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint
from typing import Optional

import requests
import validators
from dotenv import load_dotenv
from dropbox_sign import ApiClient, ApiException, Configuration, apis
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

load_dotenv()

configuration = Configuration(
    # Configure HTTP basic authorization: api_key
    username=os.getenv("DROPBOX_API_KEY"),
)

logging.basicConfig(
    filename="download.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)
logger = logging.getLogger("downloader")


@dataclass
class SignatureRequest:
    id: str
    title: str


def list_all_signature_requests(
    api_client: ApiClient,
    account_id: str = "all",
    page_size: int = 100,
    max_pages: Optional[int] = None,
    include_incomplete: bool = True,
) -> list[SignatureRequest]:
    """List out signature requests to provide main info for downloading.

    api_client: a Dropbox API client
    account_id: the account from which to download all the contracts from, if "all" then all contracts that the API user has access to
    page_size: how many items' info to download in one go, 100 is the max
    max_pages: how many pages worth of items to download, if None hten all
    include_incomplete: whether to download contracts that are marked "is_complete" : false
    """
    signature_request_api = apis.SignatureRequestApi(api_client)

    page = 1
    num_pages = max_pages

    signature_requests: list[SignatureRequest] = []

    pbar = tqdm(total=0, desc="Getting signature requests")
    while num_pages is None or page <= num_pages:
        try:
            pbar.update(1)
            response = signature_request_api.signature_request_list(
                account_id=account_id, page=page, page_size=page_size
            )
            page += 1
            # pprint(response["list_info"])
            if num_pages is None:
                num_pages = response["list_info"]["num_pages"]
                pbar.total = num_pages

            # # Debug bits
            # for request in response['signature_requests']:
            #     if request["signature_request_id"] is None:
            #         pprint(request)
            # if request['signature_request_id'] in ("4e61e789f13ba2b507a10d8a3597218b280b43d6", "335214a788cca8a376116b767d3aecded70c4033"):
            #     pprint(request)
            # if not request['is_complete']:
            #     pprint(request)

            signature_requests += [
                SignatureRequest(
                    id=request["signature_request_id"]
                    if request["signature_request_id"] is not None
                    else request["transmission_id"],
                    title=request["title"],
                )
                for request in response["signature_requests"]
                if (
                    request["signature_request_id"] is not None
                    or request["transmission_id"] is not None
                )
                and (include_incomplete or request["is_complete"])
            ]
        except ApiException as e:
            print("Exception when calling Dropbox Sign API: %s\n" % e)

    pbar.close()
    return signature_requests


def download_signature_requests(
    api_client: ApiClient,
    signature_requests: list[SignatureRequest] = [],
    download_folder: Path = Path(__file__).parent / "downloads",
    overwrite_existing: bool = True,
):
    """Download signature request files.

    api_client: a Dropbox API client
    signature_requests: a list of requests to download
    download_folder: the path to the download folder
    overwrite_existing: if True, then files are downloaded even if they exists on disk at the destination, useful to set to False when resuming a take-out
    """
    signature_request_api = apis.SignatureRequestApi(api_client)
    download_folder.mkdir(parents=True, exist_ok=True)

    s = requests.Session()

    retries = Retry(
        total=10, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
    )

    s.mount("https://", HTTPAdapter(max_retries=retries))

    for signature_requests in tqdm(signature_requests, desc="Downloading documents"):
        try:
            # Pre-generate the filename and check if it's already in the target location
            file_name = f"{signature_requests.title}_{signature_requests.id}.pdf"
            file_name = re.sub(
                "[^0-9a-zA-Z\.]+", "_", file_name
            )  # clear characters that could cause issues as a filename
            file_path = download_folder / file_name
            if Path.exists(file_path) and not overwrite_existing:
                continue

            # TODO: for draft contracts this will return a 404 which is an ApiException and thus caught, that is not great experience
            response = signature_request_api.signature_request_files_as_file_url(
                signature_requests.id
            )

            # Some sanity checking:
            if "file_url" not in response:
                logger.warning(
                    "Document id %s doesn't have a relevant document file."
                    % signature_requests.id
                )
                continue

            file_url = response["file_url"]

            if not validators.url(file_url):
                logger.error(
                    f"Not valid file url for signature '{signature_requests.title}' and ID {signature_requests.id}: {file_url}"
                )
                continue

            r = s.get(file_url, stream=True, timeout=30)
            if r.ok:
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 8):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                            os.fsync(f.fileno())
            else:  # HTTP status code 4XX/5XX
                logger.error(
                    "Download failed: status code {}\n{}".format(r.status_code, r.text)
                )

        except ApiException as e:
            logger.error(
                "Exception when calling Dropbox Sign API for id %s: %s\n"
                % (signature_requests.id, e)
            )


with ApiClient(configuration) as api_client:
    signature_requests = list_all_signature_requests(api_client=api_client)
    download_signature_requests(
        api_client, signature_requests, overwrite_existing=False
    )
