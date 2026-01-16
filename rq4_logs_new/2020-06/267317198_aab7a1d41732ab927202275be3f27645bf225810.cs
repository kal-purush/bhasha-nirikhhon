using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestQLDG
{
    [TestClass]
    public class XoaDG
    {
        private QuanLyThuVien.DAO.QLDG_DAO dg;
        private QuanLyThuVien.DTO.Account_DTO acc;
        [TestInitialize]
        public void CNXoa()
        {
            this.dg = new QuanLyThuVien.DAO.QLDG_DAO();
        }

        [TestMethod]
        public void XoaTrongMDG()
        {
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            acc.MaDocGia = "";

            Assert.AreEqual(dg.XoaDG(acc.MaDocGia), false);
        }
        [TestMethod]
        public void XoaSaiMDG()
        {
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            acc.MaDocGia = "115";

            Assert.AreEqual(dg.XoaDG(acc.MaDocGia), false);
        }
        [TestMethod]
        public void XoaMDG()
        {
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            acc.MaDocGia = "106";

            Assert.AreEqual(dg.XoaDG(acc.MaDocGia), true);
        }
    }
}