import logging
from pathlib import Path

USER_IP_ADDR_FILE = Path(".") / "user_ip_table.csv"


logging.basicConfig(level=logging.INFO)


def transform_user_ip_addr_file():
    logging.info("Transforming user_ip_table.csv")
    user_ip_addr_mapping = (
        line.split(",")
        for line in USER_IP_ADDR_FILE.absolute().read_text().splitlines()
    )
    headers = next(user_ip_addr_mapping)
    headers.append("NUMERIC_IP")
    yield headers
    yield from (row + [ipv4_to_numeric(row[-1])] for row in user_ip_addr_mapping)


def ipv4_to_numeric(ipv4_address):
    """
    Converts an IPV4 address to it's numeric representation.

    Args:
        ip_address (string): An IPV4 address e.g. 127.0.0.1

    Returns:
        int
    """
    ipv4_address_split = ipv4_address.split(".")
    assert len(ipv4_address_split) == 4, "Invalid IPV4 address"

    numeric_ip = 0
    for power, num in enumerate(reversed(ipv4_address_split)):
        numeric_ip += int(num) * pow(256, power)

    return numeric_ip
