using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Windows.Forms;

namespace Fast_Food_Restaurant.DTO
{
    public class fHoaDonNhapHangDTO
    {
        private static fHoaDonNhapHangDTO instance;

        public static fHoaDonNhapHangDTO Instance
        {
            get { if (instance == null) instance = new fHoaDonNhapHangDTO(); return instance; }

            set { instance = value; }
        }

        private string maphieu;

        public string Maphieu
        {
            get{ return maphieu;}

            set{ maphieu = value;}
        }

        private string mathucpham;

        public string Mathucpham
        {
            get { return mathucpham; }

            set { mathucpham = value; }
        }
        private string soluong;

        public string Soluong
        {
            get { return soluong; }

            set { soluong = value; }
        }

        private string dongia;

        public string Dongia
        {
            get { return dongia; }

            set { dongia = value; }
        }

        public void ThemHDNH(string a, string b, string c, DateTimePicker d,int e)
        {
            DAO.fHoaDonNhapHangDAO.Instance.ThemHoaDonNhapHang(a, b, c, d,e);

        }

        public DataGridView hienthi(string a, string b, string c, string d)
        {
            DataGridView Grid = new DataGridView();
            Grid.DataSource = DAO.fHoaDonNhapHangDAO.Instance.HienthiCTHD(a,b,c,d);
            return Grid;
        }


    }
}