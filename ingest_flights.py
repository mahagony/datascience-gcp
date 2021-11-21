"""Ingest flights datas"""

import os
import logging
import ssl
import zipfile
import gzip
import shutil
import tempfile
import datetime

from urllib.request import urlopen
from google.cloud import storage
from google.cloud.storage import Blob

def download(year, month, destdir):
    """
    Downloads on-time performance data and return local filename
    year e.g. '2015'
    month e.g. '01' for January
    """
    logging.info('Requesting data for %s-%s-*', year, month)

    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL')
    ctx_no_secure.check_hostname = False
    ctx_no_secure.verify_mode = ssl.CERT_NONE

    url = f'https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip'
    filename = os.path.join(destdir, f'{year}_{month}.zip')
    with open(filename, "wb") as file, urlopen(url, context=ctx_no_secure) as response:
        file.write(response.read())
    logging.debug('%s saved', filename)
    return filename

def zip_to_csv(filename, destdir):
    """
    Extracts the CSV file from the zip file into the destdir.
    Gzips the csv file.
    """
    with zipfile.ZipFile(filename, 'r') as zip_file:
        csv_file = zip_file.namelist()[0]
        zip_file.extract(csv_file, destdir)
    csv_file = os.path.join(destdir, csv_file)
    gzip_file = csv_file + '.gz'
    with open(csv_file, 'rb') as f_in:
        with gzip.open(gzip_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return gzip_file

def upload(file, bucketname, blobname):
    """
    Upload the file into the bucket with the given blobname
    """
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    logging.info(bucket)
    blob = Blob(blobname, bucket)
    logging.debug('Uploading %s ...', file)
    blob.upload_from_filename(file)
    gcslocation = f'gs://{bucketname}/{blobname}'
    logging.info('Uploaded %s ...', gcslocation)
    return gcslocation

def ingest(year, month, bucket):
    """
    Ingests flights data from BTS website to Google Cloud Storage
    """
    try:
        tempdir = tempfile.mkdtemp(prefix='ingest_flights')
        zip_file = download(year, month, tempdir)
        bts_csv = zip_to_csv(zip_file, tempdir)
        gcsloc = f'flights/raw/{year:d}{month:02d}.csv.gz'
        gcsloc = upload(bts_csv, bucket, gcsloc)
        return gcsloc
    finally:
        logging.debug('Cleaning up by removing %s', tempdir)
        shutil.rmtree(tempdir)

def next_month(bucketname):
    """
    Finds which months are on GCS, and return next year, month to download
    """
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blobs = list(bucket.list_blobs(prefix='flights/raw'))
    files = [blob.name for blob in blobs if blob.name.endswith('.csv.gz')]
    lastfile = os.path.basename(files[-1])
    logging.debug('The latest file on GCS is %s', lastfile)
    return compute_next_month(int(lastfile[:4]), int(lastfile[4:6]))

def compute_next_month(year, month):
    """
    Finds next month.
    """
    date = datetime.datetime(year, month, 15)
    date = date + datetime.timedelta(days=30)
    logging.debug('The next month is %s', date)
    return date.year, date.month

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='ingest filghts data from BTS website to Google Cloud Storage')
    parser.add_argument('--bucket', help='GCS bucket to upload data to', required=True)
    parser.add_argument('--year', help='Example: 2015.')
    parser.add_argument('--month', help='Specify 1 for January.')
    parser.add_argument('--debug', dest='debug', action='store_true', help='Specify if you want debug messages')

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    if args.year is None or args.month is None:
        year_, month_ = next_month(args.bucket)
    else:
        year_ = int(args.year)
        month_ = int(args.month)
    logging.debug('Ingesting year=%s, month=%s', year_, month_)
    gcs = ingest(year_, month_, args.bucket)
    logging.info('Success ... ingested to %s', gcs)
