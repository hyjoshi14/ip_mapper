import argparse
from datetime import datetime, timedelta

from ip_mapper.extractor import get_raw_ipv4_data, get_max_run_day
from ip_mapper.loader import (
    get_user_country_distribution,
    load_ipv4_data,
    load_user_country_mapping,
    load_user_ip_address_data,
)
from ip_mapper.transformer import transform_user_ip_addr_file


def not_future_date(date):
    if date > get_max_run_day():
        raise argparse.ArgumentTypeError(f"{date} cannot be after {max_run_day}")
    return date


parser = argparse.ArgumentParser(
    description=(
        "CLI to ingest IPV4 data from http://software77.net/geo-ip/"
        " and get the USER_IP_ADDRESS to COUNTRY mapping."
    )
)
parser.add_argument(
    "--date",
    dest="date",
    default=get_max_run_day(),
    help=(
        "Date for which the ETL job is to be carried out. Defaults to yesterday."
        " Should be in 'YYYY-MM-DD' format."
    ),
    type=not_future_date,
)


if __name__ == "__main__":
    args = parser.parse_args()

    raw_ipv4_data = get_raw_ipv4_data(args.date)
    load_ipv4_data(raw_ipv4_data, args.date)

    transformed_user_ip_addr_data = transform_user_ip_addr_file()
    load_user_ip_address_data(transformed_user_ip_addr_data)

    load_user_country_mapping(args.date)

    get_user_country_distribution(args.date)
