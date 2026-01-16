using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using DAO;
using DTO;

namespace BUS
{
    public class TaiXe_BUS
    {
        //Load danh sach tat ca Tai Xe
        public static DataTable LoadDanhSachTatCaTaiXe()
        {
            return TaiXe_DAO.LoadDanhSachTatCaTX();
        }

        //Them mot Tai Xe moi
        public static void ThemTaiXeMoi(TaiXe_DTO taixe)
        {
            TaiXe_DAO.ThemMotTaiXeMoi(taixe);
        }

        //Xoa mot Tai Xe
        public static void XoaTaiXe(string mataixe)
        {
            TaiXe_DAO.XoaMotTaiXe(mataixe);
        }

        //Tim theo ma Tai Xe
        public static DataTable TimTheoMaTaiXe(string mataixe)
        {
            try
            {
                return TaiXe_DAO.TimTheoMaTaiXe(mataixe);
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }

        //Tim theo ten Tai Xe
        public static DataTable TimTheoTenTaiXe(string tentaixe)
        {
            return TaiXe_DAO.TimTheoTenTaiXe(tentaixe);
        }

        //Tim Kiem Tai Xe Fullname
        public static DataTable TimKiemTX_Fullname(string fullname)
        {
            return TaiXe_DAO.TimKiemTX_Fullname(fullname);
        }

        //Kiem Tra Fullname Tai Xe Co Ton Tai
        public static int KiemTraFullnameTX(string fullname)
        {
            return TaiXe_DAO.KiemTraFullnameTX(fullname);
        }

        //Kiem tra su ton tai cua ma tai xe
        public static int KiemTraMaTX_CoTonTai(string mataixe)
        {
            return TaiXe_DAO.KiemTraMaTX_CoTonTai(mataixe);
        }

        //Kiem tra su ton tai cua ten tai xe
        public static int KiemTraTenTX_CoTonTai(string TenTX)
        {
            return TaiXe_DAO.KiemTraTenTX_CoTonTai(TenTX);
        }

        //Kiem tra su rang buoc du lieu giua Tai Xe va Xe
        public static int KiemTraTX_CoXe(string mataixe)
        {
            return TaiXe_DAO.KiemTraTX_CoXe(mataixe);
        }

        //Sua Tai Xe
        public static void SuaTaiXe(TaiXe_DTO taixe)
        {
            TaiXe_DAO.SuaTaiXe(taixe);
        }

    }

    public class KhachHang_BUS
    {
        //Load danh sach tat ca Khach Hang
        public static DataTable LoadDanhSachTatCaKhachHang()
        {
            return KhachHang_DAO.LoadDanhSachTatCaKH();
        }

        //Them mot Khach Hang moi
        public static void ThemKhachHangMoi(KhachHang_DTO khachhang)
        {
            KhachHang_DAO.ThemMotKhachHangMoi(khachhang);
        }

        //Xoa mot Khach Hang
        public static void XoaKhachHang(string makhachhang)
        {
            KhachHang_DAO.XoaMotKhachHang(makhachhang);
        }

        //Tim theo ma Khach Hang
        public static DataTable TimTheoMaKhachHang(string makhachhang)
        {
            try
            {
                return KhachHang_DAO.TimTheoMaKhachHang(makhachhang);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Tim theo ten Khach Hang
        public static DataTable TimTheoTenKhachHang(string tenkhachhang)
        {
            return KhachHang_DAO.TimTheoTenKhachHang(tenkhachhang);
        }

        //Tim Kiem Khach Hang Fullname
        public static DataTable TimKiemKH_Fullname(string fullname)
        {
            return KhachHang_DAO.TimKiemKH_Fullname(fullname);
        }

        //Kiem Tra Fullname Tai Xe Co Ton Tai
        public static int KiemTraFullnameKH(string fullname)
        {
            return KhachHang_DAO.KiemTraFullnameKH(fullname);
        }

        //Kiem tra su ton tai cua ma Khach Hang
        public static int KiemTraMaKH_CoTonTai(string maKhachHang)
        {
            return KhachHang_DAO.KiemTraMaKH_CoTonTai(maKhachHang);
        }

        //Kiem tra su ton tai cua ten Khach Hang
        public static int KiemTraTenKH_CoTonTai(string TenKH)
        {
            return KhachHang_DAO.KiemTraTenKH_CoTonTai(TenKH);
        }

        //Sua Khach Hang
        public static void SuaKhachHang(KhachHang_DTO khachhang)
        {
            KhachHang_DAO.SuaKhachHang(khachhang);
        }

        public static DataTable LayDanhSachCacChuyenDi_KH(string maKH)
        {
            try
            {
                return KhachHang_DAO.LayDanhSachCacChuyenDi_KH(maKH);
            }
            catch(Exception ex)
            {
                throw ex;
            }
            
        }
    }
}