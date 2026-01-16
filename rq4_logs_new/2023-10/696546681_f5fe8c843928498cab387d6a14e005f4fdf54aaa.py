import csv
import pwinput

# daftar barang
barang = [
    {'nama': 'samsung', 'harga': 20, 'jumlah': 12},
    {'nama': 'vivo', 'harga': 20000, 'jumlah': 23},
    {'nama': 'oppo', 'harga': 120, 'jumlah': 2}
]

# menulis data barang di csv
def dataBarang():
    # buka csv. 'w' = write
    with open('dataBarang.csv', 'w', newline='') as new_file:
        # sebagai header
        fieldNames = ['nama', 'harga', 'jumlah']
        # inisialisasi csv
        csv_writer = csv.DictWriter(
            new_file, delimiter=',', fieldnames=fieldNames)
        # nulis header
        csv_writer.writeheader()
        # ngambil data
        for i in barang:
            csv_writer.writerow(i)

dataBarang()

def tampilBarang():
    # r = read
    with open('dataBarang.csv', 'r') as baca:
        bacainData = csv.reader(baca)
        for data in bacainData:
            print(data)

tampilBarang()



def register(nama, pw):
    data = open('userdata.csv', 'r')  # r = read
    simpanUS = []
    cek = False
    for i in data:
        a, b = i.split(',')
        b = b.strip()
        simpanUS.append(a)
    if nama in simpanUS:
        print('nama udah ada')
    else:
        cek = True

    if cek:
        data = open('userdata.csv', 'a')  # a = append
        data.write('\n' + nama + ',' + pw)
        print('registrasi berhasil')
    else:
        print('registrasi gagal')


register('avivah', 'avivah')

logUser = input("Username: ")
logPW = pwinput.pwinput("Password: ", mask='*')
sukses = False
data = open("userdata.csv", "r")  # 'r' untuk read
for i in data:
    a, b = i.split(",")  # split biar bisa dipisah
    b = b.strip()
    if (a == logUser and b == logPW):
        sukses = True
        break
data.close()

if(sukses):
    print("\n--------LOGIN BERHASIL--------\n")
else:
    print('gagal')