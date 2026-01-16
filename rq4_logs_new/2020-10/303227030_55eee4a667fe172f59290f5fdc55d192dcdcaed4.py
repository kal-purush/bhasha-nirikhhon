# Pinata Cleaner by Evanito
import sys
import getopt
import requests


class PinataAPI:
    def __init__(self, api_key, api_secret):
        self.url = "https://api.pinata.cloud"
        self.headers = {
                'pinata_api_key': api_key,
                'pinata_secret_api_key': api_secret
            }
        if not self.testAuthentication():
            print('ERROR: Invalid Pinata API key or secret.')
            sys.exit(2)

    def clean(self, count="1", key=None, keyvalue=None):
        list = self.pinList()
        seen = {}
        to_unpin = []
        # check every pin in reverse chronological order
        for pin in list["rows"]:
            name = pin['metadata']['name']
            if name not in seen.keys():
                seen[name] = 1
            else:  # seen before
                if seen[name] >= int(count):  # exceeds max count
                    to_unpin.append([pin["ipfs_pin_hash"], name])
                else:  # under count
                    seen[name] += 1
        for ipfs_hash in to_unpin:
            print("Unpinning '%s' ..." % ipfs_hash[1])
            if self.unpin(ipfs_hash[0]):
                print("Unpinned successfully.")
        print("Done cleaning Pinata pins.")

    def testAuthentication(self):
        r = self.apiGet("/data/testAuthentication")
        return r["message"] == 'Congratulations! You are communicating with the Pinata API!'

    def apiGet(self, endpoint):
        url = self.url+endpoint
        r = requests.get(url=url, headers=self.headers)
        return r.json()

    def pinList(self):
        r = self.apiGet("/data/pinList?status=pinned")
        return r

    def unpin(self, ipfs_hash):
        url = self.url+"/pinning/unpin/"+ipfs_hash
        r = requests.delete(url=url, headers=self.headers)
        return r.status_code == 200


def get_args(argv):
    return_args = {}
    try:
        opts, args = getopt.getopt(argv, "ha:s:c:", ["api=", "secret=", "count="])
    except getopt.GetoptError:
        print('pinata_cleaner.py -a <api_key> -s <api_secret> -c <max_pins_with_same_name>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('pinata_cleaner.py -a <api_key> -s <api_secret> -c <max_pins_with_same_name>')
            sys.exit()
        elif opt in ("-a", "--api"):
            return_args["api_key"] = arg
        elif opt in ("-s", "--secret"):
            return_args["api_secret"] = arg
        elif opt in ("-c", "--count"):
            return_args["count"] = arg
    return return_args


if __name__ == '__main__':
    arg_dict = get_args(sys.argv[1:])
    if "api_key" not in arg_dict or "api_secret" not in arg_dict:
        print('pinata_cleaner.py -a <api_key> -s <api_secret>')
        sys.exit(2)
    Pinata = PinataAPI(arg_dict["api_key"], arg_dict["api_secret"])
    count = arg_dict["count"] if "count" in arg_dict else 1
    Pinata.clean(count=count)
