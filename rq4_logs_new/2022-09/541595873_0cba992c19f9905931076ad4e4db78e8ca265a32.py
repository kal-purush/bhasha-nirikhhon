import os

oneSectorSize = 512 ##Kích thước của một Sector

## Nhận vào tên của thư mục muốn kiểm tra, trả về MBR của thư mục đó
## Output trả về là dạng mảng
def readOneSector(filename):
    filePath = r"\\.\{0}".format(filename)
    disk_fd = os.open( filePath, os.O_RDONLY | os.O_BINARY)
    data = os.read(disk_fd, oneSectorSize)
    ##print(data)
    return data

##Duyệt sector
def indexMBR(data):
    for i in range(0,512):
        if(i % 16 == 0):
            print("\n")
        data_toHex = hex(data[i]) ##giá trị data[i] hiện tại ở dạng số từ 0 -> 255, convert về hex
        data_toHex_remove0x = data_toHex[2:] ##trả về ở lệnh trên ở dạng 0xMãHex, lệnh này để lọc bớt đi 0x
        print(data_toHex_remove0x, end = " ")

def main():
    diskName = "E:"
    data = readOneSector(diskName)
    indexMBR(data)


if(__name__ == "__main__"):
    main()