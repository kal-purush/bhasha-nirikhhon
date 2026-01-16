using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using QuanLyThuVien.DAO;
using QuanLyThuVien.DTO;

namespace QuanLyThuVien.BUS
{
    class DocGia_BUS
    {
        DocGia_DAO dgDAO = new DocGia_DAO();
        public bool SuaDG1(DocGia_DTO dg, string tdn)
        {
            if (string.IsNullOrEmpty(tdn))
                return false;
            dgDAO.SuaDG(dg, tdn);
            return true;
        }
        
    }
}