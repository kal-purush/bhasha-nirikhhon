using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    public class BanHangDTO
    {
        public int SoHoaDon { get; set; }
        public int Msnv { get; set; }
        public int MaSoBan { get; set; }
        public DateTime GioRa { get; set; }
        public string GhiChu { get; set; }
        public int ChuaThue { get; set; }
        public decimal KhuyenMai { get; set; }
        public int TongTien { get; set; }
        public DataTable ChiTiet { get; set; }
    }
}