from boto.s3.connection import S3Connection, S3ResponseError
from os.path import isfile, join, isdir
from os import listdir
import yaml, re, os, sys, getopt

# Hack for S3 Frankfurt region
os.environ['S3_USE_SIGV4'] = 'True'

ignore_files = ['.DS_Store', '.gitignore', '.htaccess', '.pdf', '.zip']
bucket = ''

def main(argv):
    print('File importer')
    helpText = 'upload-files.py <path> [-d <s3Directory>] [-r <recursiveUpload>]'
    path = ''
    s3Directory = ''
    recursiveUpload = False

    if len(argv) < 1:
        print(helpText)
        exit()
    else:
        path = argv[0]
        argv.pop(0)

    try:
        opts, args = getopt.getopt(argv, 'hd:r', ['help', 's3directory=', 'recursive'])
    except getopt.GetoptError:
        print(helpText)
        exit()

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(helpText)
            exit()
        elif opt in ('-d', '--s3directory'):
            s3Directory = arg
        elif opt in ('-r', '--recursive'):
            recursiveUpload = True

    if not isdir(path) and not isfile(path):
        print('Error: %s does not exists' % path)
        exit()
   
    config = load_config()
    connection = S3Connection(config['s3']['key_id'], config['s3']['access_key'], host=config['s3']['host'])
    global bucket
    bucket = connection.get_bucket(config['s3']['buckets']['files'])

    scan_dir(path, s3Directory, '', recursiveUpload)

    print('Finito!')
    exit()


def load_config():
    with open('config.yaml', 'r') as config_file:
        return yaml.load(config_file)


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def should_upload_file(filepath):
    if filepath in ignore_files or re.search('temp', filepath):
        return False
    else:
        return True


def upload_file_to_s3(path, s3Directory, file):
    try:
        key = s3Directory + '/' + file
        key = '/' + key.lstrip('/')

        if key_exists(key):
            print('File already uploaded', file)
            return False

        print('Uploading file', file)

        key = bucket.new_key(key)
        key.set_contents_from_filename(path + file, cb=percent_cb, num_cb=10)
    except S3ResponseError:
        print('Cannot upload the file:', sys.exc_info()[0])
    except:
        print('Unknown error:', sys.exc_info()[0])


def upload_file(path, s3Directory, file):
    if should_upload_file(file):
        upload_file_to_s3(path, s3Directory, file)
    else:
        print('Not uploading: ', file)


def key_exists(key):
    key = bucket.get_key(key)
    if not key:
        return False
    return True


def scan_dir(basepath, s3Directory, filepath, recursiveUpload):
    for file in listdir(basepath + filepath):
        if isfile(join(basepath + filepath, file)):
            upload_file(basepath, s3Directory, filepath + '/' + file)
        elif recursiveUpload == True:
            print("Continue into directory", file)
            scan_dir(basepath, s3Directory, filepath + '/' + file, recursiveUpload)


if __name__ == "__main__":
    main(sys.argv[1:])