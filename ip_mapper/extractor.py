import gzip
import logging
import re
from datetime import datetime, timedelta
from itertools import dropwhile
from tempfile import NamedTemporaryFile

import requests

logging.basicConfig(level=logging.INFO)

RAW_DATA_URL = "http://software77.net/geo-ip/history"
HEADERS = [
    "IP_FROM",
    "IP_TO",
    "REGISTRY",
    "ASSIGNED",
    "CTRY",
    "CNTRY",
    "COUNTRY",
    "REPORTED_AT",
]


def get_max_run_day():
    """Get the maximum date for which data should be available."""
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_raw_ipv4_data(date):
    """
    Obtain the raw IPV4 data for a given date.

    Args:
        date (str): 'YYYY-MM-DD' format of the date for which the data is to be obtained

    Returns:
        An iterator to the raw data
    """
    logging.info(f"Obtaining raw IPV4 data for: {date}")
    raw_data_url = get_raw_data_url(date)
    ipv4_history_page = get_ipv4_page(raw_data_url)
    file_address = get_ipv4_file_address(ipv4_history_page, date)
    raw_data = download_ipv4_data("/".join([raw_data_url, file_address]), date)
    yield HEADERS
    yield from raw_data


def get_raw_data_url(date):
    """
    Return the appropriate URL for the year to which a file belongs to.

    Args:
        date (str): 'YYYY-MM-DD' format of the date for which the data is to be obtained
    """
    archived_years = {str(year) for year in range(2011, 2016)}
    download_year = date.split("-")[0]

    if download_year in archived_years:
        return "/".join([RAW_DATA_URL, download_year])
    return RAW_DATA_URL


def get_ipv4_page(raw_data_url):
    """
    Return the content of the IPV4 data history page for a given year.

    Args:
        date (str): 'YYYY-MM-DD' format of the date for which the data is to be obtained
    """
    return requests.get(raw_data_url).text


def get_ipv4_file_address(ipv4_page_content, date):
    """
    Parse the contents of the IPV4 history page and obtain the file URL for the given date.

    Args:
        ipv4_page_content (str): Raw content of the IPV4 file archive
        date (str): 'YYYY-MM-DD' format of the date for which the data is to be obtained

    Raises:
        KeyError: When the date is not present in the file listing.
            This is possible for archived data.

        AssertionError
    """
    available_dates = re.findall(
        "(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}\s*\d\.\dM", ipv4_page_content
    )
    available_files = re.findall(
        'href="(IpToCountry\.\d{10}\.csv\.gz)', ipv4_page_content
    )

    assert len(available_dates) == len(
        available_files
    ), "Available dates and files do not match"
    available_dates_files_map = dict(zip(available_dates, available_files))

    file_address = available_dates_files_map[date]

    _, file_uploaded_at, *_ = file_address.split(".")
    file_uploaded_at = datetime.fromtimestamp(int(file_uploaded_at),) - timedelta(
        days=1
    )
    assert (
        file_uploaded_at.strftime("%Y-%m-%d") == date
    ), "The date for which the file URL is obtained is incorrect"

    return file_address


def download_ipv4_data(ipv4_file_url, date):
    """
    Downloads the IPV4 file located at the given URL.

    Args:
        ipv4_file_url (string): URL for the file to be downloaded

    Returns:
        An iterator to the contents of the file
    """
    with NamedTemporaryFile(suffix=".csv.gz") as temp_file:
        raw_data = requests.get(ipv4_file_url, stream=True)
        temp_file.write(raw_data.content)
        with gzip.open(temp_file.name, "rt", encoding="iso8859") as f:
            raw_data = dropwhile(lambda line: line.startswith("#"), f)
            rows = (re.findall('"(.*?)"', line.strip()) for line in raw_data)
            rows = (row + [date] for row in rows)
            yield from rows
