using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuanLyKhachSan
{
    class HoaDonPhong
    {
        public int maP;
        public int maHD;
        public DateTime ngayDen;
        public DateTime ngayDi;
        public bool ngay;
        public decimal thanhTien;

        public HoaDonPhong()
        {
            this.maP = 0;
            this.maHD = 0;
            this.ngayDen = DateTime.Today;
            this.ngayDi = DateTime.Today.AddDays(1);
            this.ngay = true;
            this.thanhTien = 0;
        }
        public HoaDonPhong(int maP,int maHD,DateTime ngayDen,DateTime ngayDi,bool ngay,decimal thanhTien)
        {
            this.maP = maP;
            this.maHD = maHD;
            this.ngayDen = ngayDen;
            this.ngayDi = ngayDi;
            this.ngay = ngay;
            this.thanhTien = thanhTien;
        }
        public int MaP { get => maP; set => maP = value; }
        public int MaHD { get => maHD; set => maHD = value; }
        public DateTime NgayDen { get => ngayDen; set => ngayDen = value; }
        public DateTime NgayDi { get => ngayDi; set => ngayDi = value; }
        public bool Ngay { get => ngay; set => ngay = value; }
        public decimal ThanhTien { get => thanhTien; set => thanhTien = value; }
    }
}