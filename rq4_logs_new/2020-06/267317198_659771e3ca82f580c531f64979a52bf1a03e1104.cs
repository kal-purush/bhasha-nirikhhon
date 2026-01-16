using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestQLSach
{
    [TestClass]
    public class ThemS
    {
        private QuanLyThuVien.DAO.Sach_DAO sa;
        private QuanLyThuVien.DTO.Sach_DTO s;
        
        [TestInitialize]
        public void CNThem()
        {
            this.sa = new QuanLyThuVien.DAO.Sach_DAO();
        }

        [TestMethod]
        public void TrungMS()
        {
            
            this.s = new QuanLyThuVien.DTO.Sach_DTO();
            s.MaSach = "1000";
            s.TenSach = "Bách Khoa Toàn Thư";


            Assert.AreEqual(sa.Insert(s), false);
        }
    }
}