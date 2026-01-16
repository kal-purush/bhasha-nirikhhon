using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WDFBanKinhMat.BLL
{
    class SanPham
    {
        public int MaSP { get; set; }
        public String TenSP { get; set; }
        public int MaTL { get; set; }
        public int MaCL { get; set; }
        public int MaMau { get; set; }
        public int SoLuong { get; set; }
        public String Anh { get; set; }
        public decimal DonGiaNhap { get; set; }
        public decimal DonGiaBan { get; set; }
    }
}