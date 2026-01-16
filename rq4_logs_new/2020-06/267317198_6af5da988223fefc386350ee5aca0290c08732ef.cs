using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestQLSach
{
    [TestClass]
    public class SuaS
    {
        private QuanLyThuVien.BUS.Sach_BUS sa;
        private QuanLyThuVien.DTO.Sach_DTO s;
        
        [TestInitialize]
        public void CNSua()
        {
            this.sa = new QuanLyThuVien.BUS.Sach_BUS();
        }

        [TestMethod]
        public void MPTrong()
        {
            this.dg = new QuanLyThuVien.DTO.DocGia_DTO();
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            acc.MaDocGia = "";
            Assert.AreEqual(DG.SuaDG1(dg, acc), false);

        }
    }
}