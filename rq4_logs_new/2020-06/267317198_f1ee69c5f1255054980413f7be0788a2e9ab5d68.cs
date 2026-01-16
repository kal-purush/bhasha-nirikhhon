using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuanLyThuVien.DTO;
using QuanLyThuVien.DAO;
using System.Data;

namespace QuanLyThuVien.BUS
{
    class DangKy_BUS
    {
        DangKy_DAO dkDao = new DangKy_DAO();
        //public DataTable GetList()
        //{
        //    return dkDao.loadDocGia();
        //}
        //public DataTable GetList2()
        //{
        //    return dkDao.TaiKhoan();
        //}
        //public bool DangKy(DangKy_DTO dg , DangNhap_DTO dn)
        //{
        //    if (dkDao.signup(dg, dn) == true)
        //        return true;
        //    return false;
        //}
        public bool Them(DangNhap_DTO dn, DangKy_DTO dk )
        {
            return dkDao.Insert(dn, dk);
        }
        
        //public int DangKy(DangNhap_DTO dg)
        //{
        //    if (string.IsNullOrEmpty(dg.TenDangNhap) || string.IsNullOrEmpty(dg.MatKhau))
        //        return -1;
        //    if (dkDao.signup(dg) == true)
        //        return 1;
        //    return 0;
        }
        //public bool Insert2(DangNhap_DTO dn)
        //{
        //    return dkDao.Insert2(dn);
        //}
        
        //public int Them(DangKy_DTO dg)
        //{
        //    if (string.IsNullOrEmpty(dg.MaDocGia))
        //        return 0;
        //    if (!dkDao.Insert(dg))
        //        return -1;
        //    return 1;
        //}
        //DangKy_DAO dkDAO = new DangKy_DAO();
        //public int DangKy(DangNhap_DTO _acc)
        //{
        //    if (string.IsNullOrEmpty(_acc.TenDangNhap) || string.IsNullOrEmpty(_acc.MatKhau))
        //        return -1;
        //    if (dkDAO.signup(_acc) == true)
        //        return 1;
        //    return 0;
        //}
        

        //}
        //public void sua(DangKy_DTO _dg)
        //{
        //    dkDao.Update(_dg);
        //}
    
}