"""Ingest flights datas"""

import os
import logging
import ssl
import zipfile
import gzip
import shutil

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
