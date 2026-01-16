fi = open(r"./output.bmp",'rb')
fo = open('./output1.bmp','wb')
offset4=fi.read(4)
while (len(offset4)==4):
    fo.write(offset4[:2])
    offset4 = fi.read(4)
fi.close()
fo.close() 