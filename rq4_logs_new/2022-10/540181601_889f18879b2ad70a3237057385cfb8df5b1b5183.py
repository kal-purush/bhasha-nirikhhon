from lib_certific8 import Certific8
import argparse
from colorama import init,Back,Style

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("domain",help = "Domain name to analyse")
    args = parser.parse_args()

    init(autoreset=True)
    print(Back.RED + "Cert expired                 ")
    print(Back.YELLOW + "Expiration in 15 days or less")
    print(Back.BLUE + "Error while checking cert    ")
    print("~-" * 25)

    mycerthandler = Certific8(args.domain)
    for fqdn in mycerthandler.get_fqdn_list():
        mycerthandler.get_certificate_info(fqdn)