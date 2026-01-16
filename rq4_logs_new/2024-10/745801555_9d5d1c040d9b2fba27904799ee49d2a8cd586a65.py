from faker import Faker
import random
import pyodbc

fake = Faker()

product_categories = {
    "Laptops": [
        "Laptop Dell XPS 13",
        "Laptop MacBook Pro 14",
        "Laptop HP Spectre x360",
        "Laptop ASUS ROG Zephyrus G14"
    ],
    "Smartphones": [
        "iPhone 14 Pro Max",
        "Samsung Galaxy S23 Ultra",
        "Google Pixel 8 Pro",
        "Xiaomi Mi 13"
    ],
    "Tablets": [
        "iPad Pro 12.9",
        "Samsung Galaxy Tab S9",
        "Microsoft Surface Pro 9",
        "Lenovo Tab P12"
    ],
    "Accessories": [
        "Apple AirPods Pro",
        "Samsung Galaxy Buds2",
        "Logitech MX Master 3 Mouse",
        "Anker PowerCore Portable Charger"
    ],
    "Monitors": [
        "LG UltraFine 5K",
        "Dell UltraSharp U2723QE",
        "Samsung Odyssey G9",
        "ASUS ProArt PA278QV"
    ]
}

# Tạo dữ liệu giả lập cho bảng khachHang
def insert_fake_data_khachHang(cursor, conn, n):
    for _ in range(n):
        ten = fake.name()
        email = fake.email()
        sdt = fake.phone_number()
        diachi = fake.address()
        namSinh = random.randint(1960, 2010)
        hocVan = random.choice(['Đại học', 'Cao đẳng', 'Trung học', 'Thạc sĩ', 'Tiến sĩ'])
        tinhTrangHonNhan = random.choice(['Độc thân', 'Đã kết hôn', 'Ly hôn'])
        thuNhap = random.uniform(5000000, 10000000)  # Giả lập thu nhập
        Kidhome = random.randint(0, 3)
        Teenhome = random.randint(0, 2)
        Recency = random.randint(1, 365)
        phanNan = random.choice([True, False])

        # Thực thi câu lệnh SQL để chèn dữ liệu vào bảng khachHang
        cursor.execute("""
            INSERT INTO khachHang (ten, email, sdt, diachi, namSinh, hocVan, tinhTrangHonNhan, thuNhap, Kidhome, Teenhome, Recency, phanNan)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ten, email, sdt, diachi, namSinh, hocVan, tinhTrangHonNhan, thuNhap, Kidhome, Teenhome, Recency, phanNan)
    
    conn.commit()  # Lưu thay đổi

# Hàm chèn dữ liệu vào bảng danhMucSanPham
def insert_fake_data_danhMucSanPham(cursor, conn):
    for category in product_categories.keys():
        cursor.execute("""
            INSERT INTO danhMucSanPham (tenDanhMuc)
            VALUES (?)
        """, category)
    
    conn.commit()

# Hàm chèn dữ liệu vào bảng sanPham
def insert_fake_data_sanPham(cursor, conn):
    for category, products in product_categories.items():
        # Lấy danhMucId của danh mục sản phẩm hiện tại
        cursor.execute("SELECT id FROM danhMucSanPham WHERE tenDanhMuc = ?", category)
        danhMucId = cursor.fetchone()[0]
        
        for product in products:
            gia = round(random.uniform(100, 2000), 2)  # Giá sản phẩm
            SLTonKho = random.randint(10, 200)  # Số lượng tồn kho
            moTa = fake.sentence()  # Mô tả ngẫu nhiên
            vongDoi = random.choice(['Giới thiệu', 'Phát triển', 'Trưởng thành', 'Suy thoái'])

            cursor.execute("""
                INSERT INTO sanPham (tenSanPham, danhMucId, gia, SLTonKho, moTa, vongDoi)
                VALUES (?, ?, ?, ?, ?, ?)
            """, product, danhMucId, gia, SLTonKho, moTa, vongDoi)
    
    conn.commit()

def insert_fake_data_mucTieuChienDich(cursor, conn, n):
    mucTieuChienDich = [
        'Tăng doanh thu', 
        'Tăng độ nhận diện', 
        'Tăng tỷ lệ chuyển đổi', 
        'Tạo khách hàng tiềm năng', 
        'Tăng lượng khách hàng mới', 
        'Tăng traffic website'
    ]

    for i in range(n):
        tenMucTieu = random.choice(mucTieuChienDich)  # Chọn ngẫu nhiên một mục tiêu từ danh sách
        moTa = fake.sentence()

        cursor.execute("""
            INSERT INTO mucTieuChienDich (mucTieuId, tenMucTieu, moTa)
            VALUES (?, ?, ?)
        """, i + 1, tenMucTieu, moTa)  # i + 1 để đảm bảo mucTieuId bắt đầu từ 1
    
    conn.commit()

def insert_fake_data_phanTichThiTruong(cursor, conn, n):
    phuong_phap_options = [
        "Khảo sát", "Phân tích SWOT", "Phân tích PEST", 
        "Nghiên cứu thị trường", "Phân tích đối thủ"
    ]
    
    for i in range(1, n + 1):
        phuongPhap = random.choice(phuong_phap_options)  # Chọn phương pháp ngẫu nhiên
        moTa = fake.sentence()

        cursor.execute("""
            INSERT INTO phanTichThiTruong (phanTichId, phuongPhap, moTa)
            VALUES (?, ?, ?)
        """, i, phuongPhap, moTa)
    
    conn.commit()

def insert_fake_data_kenhPhanPhoi(cursor, conn, n):
    loaiKenh_options = ["Trực tuyến", "Trực tiếp", "Đối tác", "Tổng hợp"]
    
    for i in range(1, n + 1):
        tenKenh = f"Kênh {i}"  # Tên kênh với số thứ tự
        loaiKenh = random.choice(loaiKenh_options)  # Chọn loại kênh ngẫu nhiên
        viTri = fake.address()  # Địa chỉ ngẫu nhiên
        hieuXuat = round(random.uniform(0.1, 1.0), 2)  # Hiệu suất ngẫu nhiên từ 0.1 đến 1.0
        moTa = fake.sentence()
        capDo = random.randint(1, 5)  # Cấp độ ngẫu nhiên từ 1 đến 5

        cursor.execute("""
            INSERT INTO kenhPhanPhoi (kenhId, tenKenh, loaiKenh, viTri, hieuXuat, moTa, capDo)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, i, tenKenh, loaiKenh, viTri, hieuXuat, moTa, capDo)
    
    conn.commit()

def insert_fake_data_chienDichMarketing(cursor, conn, n):
    for i in range(1, n + 1):
        tenChienDich = f"Chiến Dịch {i + 1}" 
        nganSach = random.uniform(10000, 100000)
        batDau = fake.date_this_decade()
        ketThuc = fake.date_this_decade(after_today=True)
        mucTieu = random.randint(1, 5)
        kenhPhanPhoiId = random.randint(1, 5)
        phanTichThiTruongId = random.randint(1, 5)

        cursor.execute("""
            INSERT INTO chienDichMarketing (tenChienDich, nganSach, batDau, ketThuc, mucTieu, kenhPhanPhoiId, phanTichThiTruongId)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, tenChienDich, nganSach, batDau, ketThuc, mucTieu, kenhPhanPhoiId, phanTichThiTruongId)
    
    conn.commit()

def insert_fake_data_chienLuocMarketing(cursor, conn, n):
    tenOptions = ['Marketing mix','Content Marketing','Digital Marketing', 'Marketing phân khúc', ' Marketing cạnh tranh']
    for i in range(n):
        tenChienLuoc = random.choice(tenOptions)  # Tên chiến lược với số thứ tự
        moTa = fake.sentence()
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại

        cursor.execute("""
            INSERT INTO chienLuocMarketing (chienLuocId, tenChienLuoc, moTa, chienDichId)
            VALUES (?, ?, ?, ?)
        """, i + 1, tenChienLuoc, moTa, chienDichId)  # i + 1 để đảm bảo chienLuocId bắt đầu từ 1
    
    conn.commit()

def insert_fake_data_theoDoiGiaiDoan(cursor, conn, n):
    for i in range(n):
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại
        moTa = fake.sentence()
        batDau = fake.date_time_this_year()
        ketThuc = fake.date_time_this_year(after_now=True)
        trangThai = random.choice(['Đang diễn ra', 'Hoàn thành', 'Dừng lại'])

        cursor.execute("""
            INSERT INTO theoDoiGiaiDoan (giaiDoanId, chienDichId, moTa, batDau, ketThuc, trangThai)
            VALUES (?, ?, ?, ?, ?, ?)
        """, i + 1, chienDichId, moTa, batDau, ketThuc, trangThai)  # i + 1 để đảm bảo giaiDoanId bắt đầu từ 1
    
    conn.commit()


def insert_fake_data_quangCao(cursor, conn, n):
    for i in range(n):
        tenQuangCao = f"Quảng Cáo {i + 1}"  # Tên quảng cáo với số thứ tự
        kenhPhanPhoiId = random.randint(1, 5)  # Giả định kenhPhanPhoiId đã tồn tại
        chiPhi = round(random.uniform(1000000, 50000000), 2)
        hieuXuat = random.uniform(0.1, 1.0)  # Từ 10% đến 100%
        mucTieu = random.choice(['Tăng doanh thu', 'Tăng nhận diện thương hiệu', 'Tạo khách hàng tiềm năng'])
        doiTuongKhachHang = fake.word().capitalize()  # Giả định một đối tượng khách hàng
        ngayBatDau = fake.date_this_year()
        ngayKetThuc = fake.date_this_year(after_today=True)
        kieuQuangCao = random.choice(['Banner', 'Video', 'Social Media', 'Search Engine'])
        loiKeuGoi = fake.sentence()

        cursor.execute("""
            INSERT INTO quangCao (quangCaoId, tenQuangCao, kenhPhanPhoiId, chiPhi, hieuXuat, mucTieu, doiTuongKhachHang, ngayBatDau, ngayKetThuc, kieuQuangCao, loiKeuGoi)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, i + 1, tenQuangCao, kenhPhanPhoiId, chiPhi, hieuXuat, mucTieu, doiTuongKhachHang, ngayBatDau, ngayKetThuc, kieuQuangCao, loiKeuGoi)  # i + 1 để đảm bảo quangCaoId bắt đầu từ 1
    
    conn.commit()

def insert_fake_data_nganSachMarketing(cursor, conn, n):
    for i in range(n):
        nganSachBanDau = round(random.uniform(50000000, 200000000), 2)  # Ngân sách ban đầu
        nganSachHienTai = nganSachBanDau - round(random.uniform(100000, 30000000), 2)  # Giả định một khoản chi tiêu
        chiPhiDaDung = nganSachBanDau - nganSachHienTai  # Chi phí đã dùng
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại
        kenhPhanPhoiId = random.randint(1, 5)  # Giả định kenhPhanPhoiId đã tồn tại
        quangCaoId = random.randint(1, 10)  # Giả định quangCaoId đã tồn tại
        capNhatLanCuoi = fake.date_time_this_year()
        trangThai = random.choice(['Đang hoạt động', 'Kết thúc', 'Tạm dừng'])

        cursor.execute("""
            INSERT INTO nganSachMarketing (nganSachId, nganSachBanDau, nganSachHienTai, chiPhiDaDung, chienDichId, kenhPhanPhoiId, quangCaoId, capNhatLanCuoi, trangThai)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, i + 1, nganSachBanDau, nganSachHienTai, chiPhiDaDung, chienDichId, kenhPhanPhoiId, quangCaoId, capNhatLanCuoi, trangThai)  # i + 1 để đảm bảo nganSachId bắt đầu từ 1
    
    conn.commit()


def insert_fake_data_baoCao(cursor, conn, n):
    loai_bao_cao_options = ['Hàng tuần','Hàng tháng', 'Hàng quý', 'Hàng năm']  # Các loại báo cáo
    for i in range(n):
        loaiBaoCao = random.choice(loai_bao_cao_options)
        KPI = round(random.uniform(0, 100), 2)  # KPI từ 0% đến 100%
        ROI = round(random.uniform(-1, 3), 2)  # ROI có thể âm
        thoiGian = fake.date_time_this_year()
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại
        moTa = fake.sentence()

        cursor.execute("""
            INSERT INTO baoCao (baoCaoId, loaiBaoCao, KPI, ROI, thoiGian, chienDichId, moTa)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, i + 1, loaiBaoCao, KPI, ROI, thoiGian, chienDichId, moTa)  # i + 1 để đảm bảo baoCaoId bắt đầu từ 1
    
    conn.commit()


def insert_fake_data_xuHuongThiTruong(cursor, conn, n):
    for i in range(n):
        tenXuHuong = f"Xu Hướng {i + 1}"  # Tên xu hướng với số thứ tự
        phanTich = round(random.uniform(0, 100), 2)  # Phân tích từ 0% đến 100%
        duDoan = round(random.uniform(0, 100), 2)  # Dự đoán từ 0% đến 100%
        ngayCapNhat = fake.date_time_this_year()
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại
        moTa = fake.sentence()
        nguonThamKhao = fake.company()

        cursor.execute("""
            INSERT INTO xuHuongThiTruong (xuHuongId, tenXuHuong, phanTich, duDoan, ngayCapNhat, chienDichId, moTa, nguonThamKhao)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, i + 1, tenXuHuong, phanTich, duDoan, ngayCapNhat, chienDichId, moTa, nguonThamKhao)  # i + 1 để đảm bảo xuHuongId bắt đầu từ 1
    
    conn.commit()


def insert_fake_data_phanTichROI_KPI(cursor, conn, n):
    loai_phan_tich_options = ['Hàng tháng', 'Hàng quý', 'Hàng năm']  # Các loại phân tích
    for i in range(n):
        loaiPhanTich = random.choice(loai_phan_tich_options)
        KPI = round(random.uniform(0, 100), 2)  # KPI từ 0% đến 100%
        ROI = round(random.uniform(-1, 3), 2)  # ROI có thể âm
        thoiGian = fake.date_time_this_year()
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại
        moTa = fake.sentence()

        cursor.execute("""
            INSERT INTO phanTichROI_KPI (phanTichId, loaiPhanTich, KPI, ROI, thoiGian, chienDichId, moTa)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, i + 1, loaiPhanTich, KPI, ROI, thoiGian, chienDichId, moTa)  # i + 1 để đảm bảo phanTichId bắt đầu từ 1
    
    conn.commit()

def insert_fake_data_hanhViTieuDung(cursor, conn, n):
    loai_hanh_vi_options = ['Mua sắm', 'Duyệt sản phẩm', 'Tham gia sự kiện', 'Xem quảng cáo']  # Các loại hành vi
    for i in range(n):
        moTa = fake.sentence()
        chiTiet = fake.paragraph()
        loaiHanhVi = random.choice(loai_hanh_vi_options)
        ngayGhiNhan = fake.date_time_this_year()
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại
        sanPhamId = random.randint(1, 10)  # Giả định sanPhamId đã tồn tại
        khachHangId = random.randint(1, 10)  # Giả định khachHangId đã tồn tại

        cursor.execute("""
            INSERT INTO hanhViTieuDung (hanhViId, moTa, chiTiet, loaiHanhVi, ngayGhiNhan, chienDichId, sanPhamId, khachHangId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, i + 1, moTa, chiTiet, loaiHanhVi, ngayGhiNhan, chienDichId, sanPhamId, khachHangId)  # i + 1 để đảm bảo hanhViId bắt đầu từ 1
    
    conn.commit()

def insert_fake_data_tuongTac(cursor, conn, n):
    loai_tuong_tac_options = ['Bình luận', 'Thích', 'Chia sẻ', 'Gửi tin nhắn']  # Các loại tương tác
    for i in range(n):
        khachHangId = random.randint(1, 10)  # Giả định khachHangId đã tồn tại
        thoiGian = fake.date_time_this_year()
        chiTiet = fake.sentence()
        loaiTuongTac = random.choice(loai_tuong_tac_options)
        chienDichId = random.randint(1, 10)  # Giả định chienDichId đã tồn tại
        ketQua = fake.sentence()

        cursor.execute("""
            INSERT INTO tuongTac (tuongTacId, khachHangId, thoiGian, chiTiet, loaiTuongTac, chienDichId, ketQua)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, i + 1, khachHangId, thoiGian, chiTiet, loaiTuongTac, chienDichId, ketQua)  # i + 1 để đảm bảo tuongTacId bắt đầu từ 1
    
    conn.commit()

if __name__ == "__main__":
    # Cấu hình kết nối SQL Server
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=TOBI;'    # Thay your_server bằng server của bạn
        'DATABASE=Marketing;' # Thay your_database bằng tên database
        'UID=sa;'      # Thay your_username bằng tên user
        'PWD=1234;'      # Thay your_password bằng mật khẩu của bạn
    )
    cursor = conn.cursor()



    cursor.close()
    conn.close()
