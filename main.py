"""URL trigger for ingest flights"""
import logging
from flask import escape
from ingest_flights import ingest, next_month

def ingest_flights(request):
    """Ingests flights in response to a POST"""
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    json = request.get_json(force=True)

    year = int(escape(json['year'])) if 'year' in json else None
    month = int(escape(json['month'])) if 'month' in json else None
    bucket = escape(json['bucket'])

    if year is None or month is None:
        year, month = next_month(bucket)
    logging.debug('Ingesting year=%d, month=%02d', year, month)
    gcs = ingest(year, month, bucket)
    logging.info('Success ... ingested to %s', gcs)
