using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuanLyThuVien.DTO;


namespace QuanLyThuVien.DAO
{
    class DangNhap_DAO : DataProvier
    {
        public bool login(string _tk, string _mk)
        {
            if (GetData("select* from ACCOUNT where TenDangNhap = '" + _tk + "' and MatKhau = '" + _mk + "'").Rows.Count > 0)
                return true;
            return false;
        }
        //public bool signup(Thanhvien _tv)
        //{
        //    if (GetData("select* from ACCOUNT where TenDangNhap = '" + _tv.TenDangNhap + "'").Rows.Count > 0)
        //        return false;
        //    Random rdm = new Random();
        //    _tv.MaDocGia = rdm.Next(0, 1000).ToString();
        //    while (GetData("select* from ACCOUNT where MaDocGia = '" + _tv.TenDangNhap + "'").Rows.Count > 0)
        //    {
        //        _tv.MaDocGia = rdm.Next(0, 1000).ToString();
        //    }
        //    string sql = string.Format("Insert Into ACCOUNT values('{0}','{1}','{2}','{3}')",
        //         _tv.MaDocGia, _tv.TenDangNhap, _tv.MatKhau, 1);
        //    Excute(sql);
        //    sql = string.Format("Insert Into DOCGIA(MaDocGia) values('{0}')", _tv.MaDocGia);
        //    Excute(sql);
        //    return true;
        //}
        //public void Update(DocGia _dg)
        //{
        //    string sql = string.Format("update DOCGIA set HoTen = N'{0}', GioiTinh = N'{1}', NamSinh = '{2}', DiaChi = N'{3}' where MaDocGia = '{4}'",
        //        _dg.HoTen, _dg.GioiTinh, _dg.NamSinh, _dg.DiaChi, _dg.MaDocGia);
        //    Excute(sql);
        //}
    }
}