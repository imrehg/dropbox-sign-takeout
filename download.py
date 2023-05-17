import os
import re
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint
from typing import Optional

import requests
from dotenv import load_dotenv
from dropbox_sign import ApiClient, ApiException, Configuration, apis
from tqdm import tqdm

load_dotenv()

configuration = Configuration(
    # Configure HTTP basic authorization: api_key
    username=os.getenv("DROPBOX_API_KEY"),
)


@dataclass
class SignatureRequest:
    id: str
    title: str


def list_all_signature_requests(
    api_client,
    account_id: str = "all",
    page_size: int = 100,
    max_pages: Optional[int] = None,
) -> list[SignatureRequest]:
    """List out signature requests to provide main info for downloading."""
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
            # for request in response['signature_requests']:
            #     print(f"{request['title']}: {request['signature_request_id']}")
            signature_requests += [
                SignatureRequest(
                    id=request["signature_request_id"], title=request["title"]
                )
                for request in response["signature_requests"]
            ]
        except ApiException as e:
            print("Exception when calling Dropbox Sign API: %s\n" % e)

    pbar.close()
    return signature_requests


def download_signature_requests(
    api_client,
    signature_requests: list[SignatureRequest] = [],
    download_folder: Path = Path(__file__).parent / "downloads",
):
    """Download signature request files."""
    signature_request_api = apis.SignatureRequestApi(api_client)
    download_folder.mkdir(parents=True, exist_ok=True)
    for signature_requests in tqdm(signature_requests, desc="Downloading documents"):
        try:
            response = signature_request_api.signature_request_files_as_file_url(
                signature_requests.id
            )
            file_url = response["file_url"]
            file_name = signature_requests.title + ".pdf"
            file_name = re.sub(
                "[^0-9a-zA-Z]+", "_", file_name
            )  # clear characters that could cause issues as a filename
            file_path = download_folder / file_name

            r = requests.get(file_url, stream=True, timeout=30)
            if r.ok:
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 8):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                            os.fsync(f.fileno())
            else:  # HTTP status code 4XX/5XX
                print(
                    "Download failed: status code {}\n{}".format(r.status_code, r.text)
                )

        except ApiException as e:
            print("Exception when calling Dropbox Sign API: %s\n" % e)


with ApiClient(configuration) as api_client:
    signature_requests = list_all_signature_requests(api_client=api_client)
    download_signature_requests(api_client, signature_requests)
