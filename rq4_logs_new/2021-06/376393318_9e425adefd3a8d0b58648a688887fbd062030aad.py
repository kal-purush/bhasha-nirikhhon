import datetime
import pandas as pd
import argparse
import requests
from bs4 import BeautifulSoup


def get_page(page=1):
    resp = requests.get(f"https://itviec.com/it-jobs/python?page={page}")
    soup = BeautifulSoup(resp.text, 'html.parser')
    return soup


def extract_job(jobnode):
    return jobnode.h2.text.strip()


def extract_fullpath(jobnode):
    path = jobnode.h2.a['href']
    fullpath = f"https://itviec.com{path}"
    return fullpath


def extract_address(jobnode):
    address = []
    for i in jobnode.find_all('span', class_='text'):
        address.append(i.text)
    return " - ".join(address)


def extract_distance_time(jobnode):
    return jobnode.find('span', class_='distance-time').text.strip()


def get_jobs():
    page = 1
    jobs = []
    while True:
        soup = get_page(page)
        if not soup.find('div', class_='job_content'):
            break
        for j in soup.find_all('div', class_='job_content'):
            job_info = {
                'title': extract_job(j),
                'url': extract_fullpath(j),
                'address': extract_address(j),
                'distance_time': extract_distance_time(j)
            }
            jobs.append(job_info)
        page += 1
    return jobs


def main():
    argp = argparse.ArgumentParser()
    argp.add_argument("-l", "--location", choices=["HN", "HCM"], type=str)
    args = argp.parse_args()

    jobs = get_jobs()
    today = datetime.date.today().strftime("%Y%m%d")
    df = pd.DataFrame(jobs)
    df.to_csv(f'{today}_itviec_jobs')

    if args.location == "HN":
        location_filter = "Ha Noi"
    elif args.location == "HCM":
        location_filter = "Ho Chi Minh"
    else:
        location_filter = ""

    jobs = (j for j in jobs if location_filter in j["address"])
    for idx, job in enumerate(jobs, start=1):
        print(
            "{idx:>2}. {distance_time:<12} | {address:<25} | {title:<50} | {url:<40}".format(
                idx=idx, **job
            )
        )


if __name__ == '__main__':
    main()