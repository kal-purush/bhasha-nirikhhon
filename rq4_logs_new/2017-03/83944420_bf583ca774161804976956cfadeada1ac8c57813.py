import re
import os
import sys
import urllib.request
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas

def open_url(url):
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36")
    page = urllib.request.urlopen(req)
    html = page.read().decode("utf-8")
    # print(html)
    return html

def get_img_url(html):
    # 获取课本名称列表
    pattern1 = r'<img data-original.+?alt="(.+?)">'
    img_name_list = re.findall(pattern1,html)

    # <img data-original="http://keben.100qingda.com/1/143xx/thumb/0001.jpg" alt="人教版pep 四年级下 英语">
    # 获取图片地址列表
    pattern2 = r'<a href="(http://keben.100qingda.com.+?/bigImg/.+?\.jpg)">'
    img_url_list = re.findall(pattern2,html)

    # print(img_url_list)
    try:
        global folder_name
        folder_name = img_name_list[0]
        os.mkdir(folder_name)
    except FileExistsError:
        pass
    os.chdir(folder_name)
    for each in img_url_list:
        file_name = each.split("/")[-1]
        urllib.request.urlretrieve(each, file_name, None)
    return

def pic_to_pdf(pic_dir,pdf_file_name):
    file_name_list = []
    for each in os.listdir(pic_dir):
        if each.split(".")[-1] == "jpg" or each.split(".")[-1] == "png":
            file_name_list.append(each)
    # 当分辨率是72像素/英寸时，A4纸像素长宽分别是842×595px,landscape(A4)=(841.8897637795275, 595.275590551181)
    (height, width) = landscape(A4)
    c = canvas.Canvas(pdf_file_name, pagesize=(width, height))
    for each_pic in file_name_list:
        each_pic_path = pic_dir+"\\"+each_pic
        c.drawImage(each_pic_path, 0, 0, width, height)
        # c.drawImage(each_pic_path, 0, 0)
        c.showPage()
    c.save()
    return

def main():
    url = input("请输入课本所在的页面地址：\n")
    print("正在下载…")
    get_img_url(open_url(url))
    print("下载完毕！")
    print("正在转换成pdf…")
    pic_dir = os.getcwd()
    pdf_name = folder_name + r".pdf"
    pic_to_pdf(pic_dir, pdf_name)
    return

main()



