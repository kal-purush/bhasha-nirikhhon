import io
import re
from urllib.request import urlopen
import zipfile

from s3fs.core import S3FileSystem


from summarize import mbta_feed_url_for, process_file, CURRENT_FEED_URL


def mbta_feed_for(when):
    url = mbta_feed_url_for(when) or CURRENT_FEED_URL
    with urlopen(url) as u:
        return zipfile.ZipFile(io.BytesIO(u.read()))


def summarize(event, context):
    record = next((rec["s3"] for rec in event["Records"]
                   if "s3" in rec), None)
    if not record:
        return

    key = record["object"]["key"]
    bucket = record["bucket"]["name"]
    m = re.match(r"\d{4}/\d{2}/\d{4}-\d{2}-\d{2}\.csv\.gz$", key)
    if not m:
        return

    print(f"File {key} uploaded.")

    df = process_file(f"s3://{bucket}/{key}", mbta_feed_for)
    s3 = S3FileSystem()

    filename = key[0:-3]
    with s3.open(f"{bucket}/summary/{filename}", "w") as f:
        df.to_csv(f)