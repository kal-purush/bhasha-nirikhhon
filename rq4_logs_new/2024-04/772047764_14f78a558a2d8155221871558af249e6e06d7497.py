import requests
from pathlib import Path

# 假设你有一个包含下载链接的 list
urls = [
    'https://raw.githubusercontent.com/Efan3536/sdyun/main/config.json',
    'https://raw.githubusercontent.com/Efan3536/sdyun/main/ui-config.json',
    # 更多文件链接...
]

# 保存文件的根目录
save_dir = '/root/sdw/'

# 确保 save_dir 存在
Path(save_dir).mkdir(parents=True, exist_ok=True)

# 批量下载文件
for url in urls:
    # 从 URL 中分解出文件名
    filename = url.split('/')[-1]

    # 完整的保存路径
    save_path = Path(save_dir) / filename

    # 显示我们正在下载的文件
    print(f"Downloading {url}...")

    # 发起请求下载文件
    response = requests.get(url)
    response.raise_for_status()  # 检查请求是否成功

    # 写入文件到指定路径
    with save_path.open('wb') as file:
        file.write(response.content)

    print(f"Saved to {save_path}")

print("All files have been downloaded.")