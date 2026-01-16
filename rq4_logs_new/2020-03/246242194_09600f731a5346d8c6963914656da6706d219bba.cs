using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TTN_QL_HSGV.DTO;
using TTN_QL_HSGV.DAL;
using System.Data;

namespace TTN_QL_HSGV.BUS
{
    class GiaoVienBUS
    {
        GiaoVienDAL gv = new GiaoVienDAL();
        public bool ThemGV(GiaoVien giaoVien)
        {   
            return gv.ThemGV(giaoVien);
        }

        public bool SuaGV(GiaoVien giaoVien)
        {
            return gv.SuaGV(giaoVien);
        }

        public List<GiaoVien> XemTatCaGV()
        {
            return gv.XemTatCaGV();
        }

        public GiaoVien XemChiTietGV(int maGV)
        {
            return gv.XemChiTietGV(maGV);
        }

        public DataTable LayChucVu()
        {
            return gv.LayChucVu();
        }

        public List<GiaoVien> SearchGiaoVien(string gioiTinh, string chucVu, int maMon)
        {
            return gv.SearchGiaoVien(gioiTinh, chucVu, maMon);
        }
    }
}