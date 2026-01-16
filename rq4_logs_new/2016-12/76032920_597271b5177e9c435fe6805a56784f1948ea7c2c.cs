using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DAL;
using DTO;

namespace BLL
{
    public class Business
    {
        #region Nhân viên
        public DataTable GetDataNV()
        {
            return new NhanVien_DAL().GetDataNV();
        }

        public Boolean ThemNV(NhanVien_DTO nv)
        {
            try
            {
                if (nv.IDNhanVien == "")
                {
                    throw new Exception("Field \"IDNhanVien\" không thể trống.");
                }
                return new NhanVien_DAL().ThemNV(nv);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaNV(NhanVien_DTO nv)
        {
            try
            {
                if (nv.IDNhanVien == "")
                {
                    throw new Exception("Field \"IDNhanVien\" không thể trống.");
                }
                return new NhanVien_DAL().SuaNV(nv);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaNV(string idNhanVien)
        {
            try
            {
                if (idNhanVien == "")
                {
                    throw new Exception("Field \"IDNhanVien\" không thể trống.");
                }
                return new NhanVien_DAL().XoaNV(idNhanVien);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Khách hàng
        public DataTable GetDataKH()
        {
            return new KhachHang_DAL().GetDataKH();
        }

        public Boolean ThemKH(KhachHang_DTO kh)
        {
            try
            {
                if (kh.IDKhachHang == "")
                {
                    throw new Exception("Field \"IDKhachHang\" không thể trống.");
                }
                return new KhachHang_DAL().ThemKH(kh);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaKH(KhachHang_DTO kh)
        {
            try
            {
                if (kh.IDKhachHang == "")
                {
                    throw new Exception("Field \"IDKhachHang\" không thể trống.");
                }
                return new KhachHang_DAL().SuaKH(kh);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaKH(string idKhachHang)
        {
            try
            {
                if (idKhachHang == "")
                {
                    throw new Exception("Field \"IDKhachHang\" không thể trống.");
                }
                return new KhachHang_DAL().XoaKH(idKhachHang);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Phòng
        public DataTable GetDataPhong()
        {
            return new Phong_DAL().GetDataPhong();
        }

        public Boolean ThemPhong(Phong_DTO p)
        {
            try
            {
                if (p.IDPhong == "")
                {
                    throw new Exception("Field \"IDPhong\" không thể trống.");
                }
                return new Phong_DAL().ThemPhong(p);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaPhong(Phong_DTO p)
        {
            try
            {
                if (p.IDPhong == "")
                {
                    throw new Exception("Field \"IDPhong\" không thể trống.");
                }
                return new Phong_DAL().SuaPhong(p);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaPhong(string idPhong)
        {
            try
            {
                if (idPhong == "")
                {
                    throw new Exception("Field \"IDPhong\" không thể trống.");
                }
                return new Phong_DAL().XoaPhong(idPhong);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Loại Phòng
        public DataTable GetDataLP()
        {
            return new LoaiPhong_DAL().GetDataLP();
        }

        public DataTable GetDataLP_IDLP(string idLoaiPhong)
        {
            return new LoaiPhong_DAL().GetDataLP_IDLP(idLoaiPhong);
        }

        public DataTable GetDataLP_fromIDPhong(string idPhong)
        {
            return new LoaiPhong_DAL().GetDataLP_fromIDPhong(idPhong);
        }

        public Boolean ThemLP(LoaiPhong_DTO lp)
        {
            try
            {
                if (lp.IDLoaiPhong == "")
                {
                    throw new Exception("Field \"IDLoaiPhong\" không thể trống.");
                }
                return new LoaiPhong_DAL().ThemLP(lp);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaLP(LoaiPhong_DTO lp)
        {
            try
            {
                if (lp.IDLoaiPhong == "")
                {
                    throw new Exception("Field \"IDLoaiPhong\" không thể trống.");
                }
                return new LoaiPhong_DAL().SuaLP(lp);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaLP(string idLoaiPhong)
        {
            try
            {
                if (idLoaiPhong == "")
                {
                    throw new Exception("Field \"IDLoaiPhong\" không thể trống.");
                }
                return new LoaiPhong_DAL().XoaLP(idLoaiPhong);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Đặt phòng
        public DataTable GetDataDP()
        {
            return new DatPhong_DAL().GetDataDP();
        }

        public DataTable GetDataDP(string idDatPhong)
        {
            return new DatPhong_DAL().GetDataDP(idDatPhong);
        }

        public DataTable GetDataDP_fromMaKH(string maKH)
        {
            return new DatPhong_DAL().GetDataDP_fromMaKH(maKH);
        }

        public Boolean ThemDP(DatPhong_DTO dp)
        {
            try
            {
                if (dp.MaNV == "Chọn mã NV")
                {
                    throw new Exception("Chưa chọn mã nhân viên.");
                }
                else if (dp.MaKH == "Chọn mã KH")
                {
                    throw new Exception("Chưa chọn mã khách hàng.");
                }
                else if (dp.MaPhong == "Chọn mã phòng")
                {
                    throw new Exception("Chưa chọn mã phòng.");
                }
                bool status = new DatPhong_DAL().ThemDP(dp);
                if (status == false)
                {
                    throw new Exception("Phòng đã được đặt.");
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaDP(DatPhong_DTO dp)
        {
            try
            {
                return new DatPhong_DAL().SuaDP(dp);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaDP(string idDatPhong)
        {
            try
            {
                return new DatPhong_DAL().XoaDP(idDatPhong);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Hình ảnh

        #endregion

        #region Cơ sở vật chất
        public DataTable GetDataCSVC()
        {
            return new CoSoVatChat_DAL().GetDataCSVC();
        }

        public Boolean ThemCSVC(CoSoVatChat_DTO csvc)
        {
            try
            {
                if (csvc.IDCSVC == "")
                {
                    throw new Exception("Field \"IDCSVC\" không thể trống.");
                }
                return new CoSoVatChat_DAL().ThemCSVC(csvc);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaCSVC(CoSoVatChat_DTO csvc)
        {
            try
            {
                if (csvc.IDCSVC == "")
                {
                    throw new Exception("Field \"IDCSVC\" không thể trống.");
                }
                return new CoSoVatChat_DAL().SuaCSVC(csvc);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaCSVC(string idCSVC)
        {
            try
            {
                if (idCSVC == "")
                {
                    throw new Exception("Field \"IDCSVC\" không thể trống.");
                }
                return new CoSoVatChat_DAL().XoaCSVC(idCSVC);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Thực phẩm
        public DataTable GetDataTP()
        {
            return new ThucPham_DAL().GetDataTP();
        }

        public DataTable GetDataTP(string idThucPham)
        {
            return new ThucPham_DAL().GetDataTP(idThucPham);
        }

        public Boolean ThemTP(ThucPham_DTO tp)
        {
            try
            {
                if (tp.IDThucPham == "")
                {
                    throw new Exception("Field \"IDThucPham\" không thể trống.");
                }
                return new ThucPham_DAL().ThemTP(tp);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaTP(ThucPham_DTO tp)
        {
            try
            {
                if (tp.IDThucPham == "")
                {
                    throw new Exception("Field \"IDThucPham\" không thể trống.");
                }
                return new ThucPham_DAL().SuaTP(tp);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaTP(string idThucPham)
        {
            try
            {
                if (idThucPham == "")
                {
                    throw new Exception("Field \"IDThucPham\" không thể trống.");
                }
                return new ThucPham_DAL().XoaTP(idThucPham);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Hóa đơn thực phẩm

        #endregion

        #region Hướng dẫn viên

        #endregion

        #region Dịch vụ HDV
        public DataTable GetDataDVhdv()
        {
            return new DVhdv_DAL().GetDataDVhdv();
        }

        public Boolean ThemDVhdv(DVhdv_DTO DVhdv)
        {
            try
            {
                if (DVhdv.IDDVhdv == "")
                {
                    throw new Exception("Field \"IDDVhdv\" không thể trống.");
                }
                return new DVhdv_DAL().ThemDVhdv(DVhdv);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaDVhdv(DVhdv_DTO DVhdv)
        {
            try
            {
                if (DVhdv.IDDVhdv == "")
                {
                    throw new Exception("Field \"IDDVhdv\" không thể trống.");
                }
                return new DVhdv_DAL().SuaDVhdv(DVhdv);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaDVhdv(string idDVhdv)
        {
            try
            {
                if (idDVhdv == "")
                {
                    throw new Exception("Field \"IDDVhdv\" không thể trống.");
                }
                return new DVhdv_DAL().XoaDVhdv(idDVhdv);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Xe
        public DataTable GetDataXe()
        {
            return new Xe_DAL().GetDataXe();
        }

        public Boolean ThemXe(Xe_DTO xe)
        {
            try
            {
                if (xe.IDXe == "")
                {
                    throw new Exception("Field \"IDXe\" không thể trống.");
                }
                return new Xe_DAL().ThemXe(xe);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean SuaXe(Xe_DTO xe)
        {
            try
            {
                if (xe.IDXe == "")
                {
                    throw new Exception("Field \"IDXe\" không thể trống.");
                }
                return new Xe_DAL().SuaXe(xe);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Boolean XoaXe(string idXe)
        {
            try
            {
                if (idXe == "")
                {
                    throw new Exception("Field \"IDXe\" không thể trống.");
                }
                return new Xe_DAL().XoaXe(idXe);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Thuê xe

        #endregion
    }
}