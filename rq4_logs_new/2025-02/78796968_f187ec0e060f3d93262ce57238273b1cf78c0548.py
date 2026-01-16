import ipaddress
import sys
from tenable.io import TenableIO

def main():
    tio_access_key = input("TIO_ACCESS_KEY")
    tio_secret_key = input("TIO_SECRET_KEY")
    cidr = input("CIDR")

    tenable_vendor = ""
    tenable_product = ""
    tenable_version = ""

    tio = TenableIO(
        tio_access_key,
        tio_secret_key,
        vendor=tenable_vendor,
        product=tenable_product,
        build=tenable_version,
    )

    cidr_obj = ipaddress.ip_network(cidr)

    print(cidr_obj)

#    results = tio.exports.vulns(plugin_id=plugin_ids, cidr_range=config.TARGET_CIDR)
    results = tio.exports.vulns(cidr_range=cidr)

    for r in results:
        print(r)
        print("-----------------")
        print()


if __name__ == '__main__':
    sys.exit(main())
