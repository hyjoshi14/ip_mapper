## IP Mapper

This repository obtains data from http://software77.net/geo-ip/history/
and helps understand the distribution of IP addresses by Country.

### Sections
- [Prerequisites](#Prerequisites)
- [Installation](#Installation)
- [About the data](#About-the-data)
- [Using IP Mapper](#Using-IP-Mapper)


### Prerequisites
- [Python 3.7](https://www.python.org/downloads/release/python-376/)
- [SQLite](https://www.sqlite.org/download.html)


### Installation
1. Clone/Download the repo into your machine
2. Switch to the directory where the repo is located and set up a virtual environment
```
cd ~/path-to-ip-mapper/
virtualenv .
```
3. Activate the virtualenv
```
source bin/activate
```
4. Install the packages in the `requirements.txt` file
```
pip install -r requirements.txt
```

### About the data
The IPV4 files are located [here](http://software77.net/geo-ip/history/).

There exists archived data for the years 2011 to 2015.
From 2016 onwards, daily files are uploaded.

Each file contains data following the below listed description.
```
# IP FROM      IP TO        REGISTRY  ASSIGNED   CTRY CNTRY COUNTRY
# "1346797568","1346801663","ripencc","20010601","il","isr","Israel"
#
# IP FROM & : Numerical representation of IP address.
# IP TO       Example: (from Right to Left)
#             1.2.3.4 = 4 + (3 * 256) + (2 * 256 * 256) + (1 * 256 * 256 * 256)
#             is 4 + 768 + 13,1072 + 16,777,216 = 16,909,060
#
# REGISTRY  : apnic, arin, lacnic, ripencc and afrinic
#             Also included as of April 22, 2005 are the IANA IETF Reserved
#             address numbers. These are important since any source claiming
#             to be from one of these IPs must be spoofed.
#
# ASSIGNED  : The date this IP or block was assigned. (In Epoch seconds)
#             NOTE: Where the allocation or assignment has been transferred from
#                   one registry to another, the date represents the date of first
#                   assignment or allocation as received in from the original RIR.
#                   It is noted that where records do not show a date of first
#                   assignment, the date is given as "0".
#
# CTRY      : 2 character international country code
#             NOTE: ISO 3166 2-letter code of the organisation to which the
#             allocation or assignment was made, and the enumerated variances of:
#                  AP - non-specific Asia-Pacific location
#                  CS - Serbia and Montenegro
#                  YU - Serbia and Montenegro (Formally Yugoslavia) (Being phased out)
#                  EU - non-specific European Union location
#                  FX - France, Metropolitan
#                  PS - Palestinian Territory, Occupied
#                  UK - United Kingdom (standard says GB)
#                * ZZ - IETF RESERVED address space.
```

### Using IP Mapper
To use IP Mapper to ingest data and run validations, use the following command.

Example: Ingesting data for 1st Jan 2020.
```
python etl.py --date '2020-01-01'
```

To learn more about the CLI, run
```
python etl.py --help
```
