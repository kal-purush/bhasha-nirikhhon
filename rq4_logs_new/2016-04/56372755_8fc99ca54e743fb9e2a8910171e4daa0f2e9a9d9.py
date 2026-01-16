import boto

from boto.s3.key import Key


def download(bucket, src, dest, **aws_config):
    conn = boto.connect_s3(aws_config['access'], aws_config['secret'])
    key = Key(conn.get_bucket(bucket), src)

    f = open(dest, 'wb')
    key.get_contents_to_file(f)
    f.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description='Download from S3.')
    p.add_argument('--bucket', '-b', required=True, dest="bucket", help="The bucket")
    p.add_argument('--resource', '-r', required=True, dest="resource", help="The resource to download from the bucket")
    p.add_argument('--dest', '-d', required=True, dest="dest", help="The destination path for the download")
    p.add_argument('--access', '-a', default=None, dest="access", help="The AWS access key")
    p.add_argument('--secret', '-s', default=None, dest="secret", help="The AWS secret key")
    args = p.parse_args()

    access = args.access
    if not access:
        import aws_settings
        access = aws_settings.KEY

    secret = args.secret
    if not secret:
        secret = aws_settings.SECRET

    download(args.bucket, args.resource, args.dest, access=access, secret=secret)