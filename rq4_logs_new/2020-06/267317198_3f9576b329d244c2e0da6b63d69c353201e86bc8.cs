using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using QuanLyThuVien.DTO;
using QuanLyThuVien.DAO;
using System.Data;

namespace QuanLyThuVien.BUS
{
    class PhieuMuon_BUS
    {
        PhieuMuon_DAO muonDao = new PhieuMuon_DAO();
        public DataTable GetList()
        {
            return muonDao.Phieumuon();
        }
        public void XoaM(string mS)
        {
            muonDao.Xoa(mS);
        }
        public bool SuaM(PhieuMuon_DTO pm)
        {
            if (string.IsNullOrEmpty(pm.MaPhieu))
                return false;
            muonDao.Sua(pm);
            return true;
        }
        public int ThemM(PhieuMuon_DTO pm)
        {
            if (string.IsNullOrEmpty(pm.MaPhieu))
                return 0;
            if (!muonDao.Them(pm))
                return -1;
            return 1;
        }
        public DataTable TimKiemM(string _timkiem, string _loaitk)
        {
            return muonDao.Tim(_timkiem, _loaitk);
        }
    }
}
