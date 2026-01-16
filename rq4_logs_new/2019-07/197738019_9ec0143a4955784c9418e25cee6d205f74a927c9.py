import re

def readOp(url):
    with open(url,'r') as f:
        data = []
        while True:
            i=0
            lines = f.readline()  # 整行读取数据
            i=i+1
            if not lines:
                break
            if(i!=0):
                dataline=[]
                lines = f.readline()
                texts = lines.split(' ')
                # print(text)
                # break
                for text in texts:
                    if text==' ' or text=='':
                        continue
                    else:
                        result = re.match(r"[0-9]*.?[0-9]*", text)
                        try:
                            dataline.append(float(result.group()))
                        except:
                            print(result)
                process_dataline=[]
                for i in range(len(dataline)):
                    if i not in (0,1,2,4,6,8,10,12,14):
                        process_dataline.append(dataline[i])
                data.append(process_dataline)
        return data
