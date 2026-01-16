from datetime import datetime, timedelta

now = datetime.now()  # 获取当前时间
fivedaygetime = now - timedelta(days=5)  # 获取5天前的时间
# 生成文件名
time_file_name = 'save_fivedayago_time_'+ now.strftime('%Y-%m-%d_%H-%M-%S') + '.txt'
print(time_file_name)
time_file = open(time_file_name, 'w')  # 写入文件
time_file.write(str(fivedaygetime))
time_file.close()

if __name__ == '__main__':
    pass