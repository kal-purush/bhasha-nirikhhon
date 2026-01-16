using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuanLyThuVien.DTO;
using QuanLyThuVien.DAO;
using System.Data.SqlClient;
using System.Data;

namespace QuanLyThuVien.BUS
{
    class DoiMK_BUS
    {
        DoiMK_DAO mkDao = new DoiMK_DAO();
        public void DoiMatKhau(Account_DTO mk)
        {
            mkDao.DoiMK(mk);
        }
        public bool CheckExist(string _tdn, string _mkc)
        {
            return mkDao.CheckExist(_tdn, _mkc);
        }

    }
}