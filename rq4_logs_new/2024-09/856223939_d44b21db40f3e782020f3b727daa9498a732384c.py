# coding: utf-8

import os
import time
import pathlib
import requests
from hashlib import md5
from urllib import parse
from m3u8 import M3U8
from concurrent.futures import ThreadPoolExecutor, as_completed


def timer(func):
    def wrapper(*args, **kwargs):
        def time_now(): return time.time()
        time_start = time_now()
        result = func(*args, **kwargs)
        time_spend = time_now() - time_start
        print(f"{func.__name__} cost time: {time_spend:.3f} s")
        return result
    return wrapper


def calculate_md5(file_path):
    with open(file_path, "rb") as fp:
        result = md5(fp.read()).hexdigest()
    return result


@timer
# @retry
def down_to_m3u8_video(url=None, headers=None, timeout=10, video_name=None):
    r = requests.get(url=url, headers=headers, timeout=timeout)
    r.raise_for_status()

    if r.status_code != 200:
        raise ValueError(r.status_code)

    if r.status_code == 200:
        hash1 = 1
        hash2 = 2

        # 校验MD5会增加时长 不需要可以删除
        hash1 = md5(r.content).hexdigest()
        if os.path.isfile(video_name):
            hash2 = calculate_md5(video_name)
        else:
            with open(video_name, "wb") as fp:
                fp.write(r.content)
            hash2 = calculate_md5(video_name)

        if hash1 == hash2:
            print(f"success {url}")
        else:
            print(f"failure {url}")
    return r


def down_m3u8_index(url=None, headers=None, timeout=10, m3u8_path=None):
    response = requests.get(url=url, headers=headers, timeout=timeout)
    with open(m3u8_path, "w") as fp:
        fp.write(response.text)
    return response


def mk_video_dir(url):
    dir_string = parse.urlparse(url)
    base_dir = os.path.abspath(os.path.dirname(__file__))
    video_dir = base_dir + os.path.dirname(dir_string.path)
    pathlib.Path(video_dir).mkdir(parents=True, exist_ok=True)
    return video_dir


def url_to_video_name(url, uri):
    video_name = os.path.basename(parse.urljoin(url, uri))
    return video_name


def url_to_new_url(url, uri):
    new_url = parse.urljoin(url, uri)
    return new_url


def to_m3u8_index_url(m3u8_file):
    with open(m3u8_file, "r") as fp:
        m3u8_index_url = fp.read()
    return m3u8_index_url


def http_config():
    config = {
        "timeout": 10,
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.1.4753.73 Safari/527.56 Edg/102.1.4753.73",
   	        "Accept-Encoding": "identity"
        }
    }
    return config


def main():
    config = http_config()

    # 获取绝对路径
    base_dir = os.path.abspath(os.path.dirname(__file__))

    # 从文件读取URL
    url_file = "url.txt"
    m3u8_index_url = to_m3u8_index_url(base_dir + "/" + url_file)

    # 根据URL创建目录
    # video_dir = mk_video_dir(m3u8_index_url)

    # 创建目录
    video_dir =  base_dir + "/" + "videos"
    pathlib.Path(video_dir).mkdir(parents=True, exist_ok=True)

    # 下载m3u8索引文件并保存为本地文件
    m3u8_path = video_dir + "/" + "video.m3u8"
    response = down_m3u8_index(m3u8_index_url.strip("\n"), headers=config["headers"], timeout=config["timeout"], m3u8_path=m3u8_path)

    # m3u8媒体播放文件的URL列表
    m3u8_obj = M3U8(response.text)
    videos = ({"url": url_to_new_url(m3u8_index_url, uri), "video_name": video_dir + "/" + url_to_video_name(m3u8_index_url, uri)} for uri in m3u8_obj.segments.uri)

    now = lambda: time.time()
    start = now()

    # 线程数量
    workers = 32  
    
    # 多线程任务
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futs = (executor.submit(down_to_m3u8_video, video["url"], config["headers"], config["timeout"], video["video_name"]) for video in videos)
        for fut in as_completed(futs):
            # result = fut.result()
            # print(result.status_code)
            pass

    print(f"Total time: {now() - start} s")


if __name__ == "__main__":
    main()
    