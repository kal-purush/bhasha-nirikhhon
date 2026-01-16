using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestDangNhapvaDangKy
{
    [TestClass]
    public class DangKy
    {
        private QuanLyThuVien.DAO.DangKy_DAO dk;
        private QuanLyThuVien.DTO.Account_DTO acc;
        private QuanLyThuVien.DTO.DocGia_DTO dg;
        [TestInitialize]
        public void CNDangKy()
        {
            this.dk = new QuanLyThuVien.DAO.DangKy_DAO();
        }

        [TestMethod]
        public void TrungMDG()
        {
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            this.dg = new QuanLyThuVien.DTO.DocGia_DTO();
            acc.MaDocGia = "776";
            acc.TenDangNhap = "KimNgan";
            acc.MatKhau = "KN123";
            acc.Quyen = 0;
            dg.MaDocGia = acc.MaDocGia;
            dg.TenDangNhap = acc.TenDangNhap;
            dg.HoTen = "Nguyễn Kim Ngân";
            dg.DiaChi = "23 Nguyễn Kiệm";
            dg.GioiTinh = "Nữ";

            Assert.AreEqual(dk.Insert(acc, dg), false);

        }

        [TestMethod]
        public void DK()
        {
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            this.dg = new QuanLyThuVien.DTO.DocGia_DTO();
            acc.MaDocGia = "126";
            acc.TenDangNhap = "KimNgan";
            acc.MatKhau = "KN123";
            acc.Quyen = 0;
            dg.NamSinh = DateTime.Now;
            dg.HoTen = "Lê Kim Ngân";
            dg.DiaChi = "23 Nguyễn Kiệm";
            dg.GioiTinh = "Nữ";

            Assert.AreEqual(dk.Insert(acc, dg), true);

        }
        [TestMethod]
        public void MKInHoa()
        {
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            this.dg = new QuanLyThuVien.DTO.DocGia_DTO();
            acc.MaDocGia = "4567";
            acc.TenDangNhap = "hathu";
            acc.MatKhau = "123";
            acc.Quyen = 0;
            dg.NamSinh = DateTime.Now;
            dg.HoTen = "Lê Kim Ngân";
            dg.DiaChi = "23 Nguyễn Kiệm";
            dg.GioiTinh = "Nữ";

            Assert.AreEqual(dk.Insert(acc, dg), false );

        }
       
        [TestMethod]
        public void TrungTenDN()
        {
            this.acc = new QuanLyThuVien.DTO.Account_DTO();
            this.dg = new QuanLyThuVien.DTO.DocGia_DTO();
            acc.MaDocGia = "412";
            acc.TenDangNhap = "hathu";
            acc.MatKhau = "123L";
            acc.Quyen = 0;
            dg.NamSinh = DateTime.Now;
            dg.HoTen = "Lê Kim Ngân";
            dg.DiaChi = "23 Nguyễn Kiệm";
            dg.GioiTinh = "Nữ";

            Assert.AreEqual(dk.Insert(acc, dg), false);

        }

    }
}