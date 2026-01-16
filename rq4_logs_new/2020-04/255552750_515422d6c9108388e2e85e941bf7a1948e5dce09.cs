using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace QuanLyKhachSan
{
    class Control_DVDD
    {
        public List<DoDung> ConvertDD(DataTable datatable)
        {
            List<DoDung> dds = new List<DoDung>();
            dds = (from DataRow dr in datatable.Rows
                    select new DoDung()
                    {
                        MaDD = dr["MaDD"].ToString(),
                        TenDD = dr["TenDD"].ToString(),
                        Gia = (decimal)dr["Gia"],
                        DonVi = dr["DonVi"].ToString()
                    }).ToList();
            return dds;
        }
        public List<DichVu> ConvertDV(DataTable datatable)
        {
            List<DichVu> dvs = new List<DichVu>();
            dvs = (from DataRow dr in datatable.Rows
                   select new DichVu()
                   {
                       MaDV = dr["MaDV"].ToString(),
                       TenDV = dr["TenDV"].ToString(),
                       Gia = (decimal)dr["Gia"]
                   }).ToList();
            return dvs;
        }
        public List<DichVu> GetDanhSachDichVu()
        {
            //viết proc lấy ds dịch vụ
            DataTable data;
            string query = "select * from DichVu";
            data = DataProvider.Instance.ExecuteQuery(query);
            return ConvertDV(data);
        }
        public List<DoDung> GetDanhSachDichDung()
        {
            //viết proc lấy ds đồ dùng
            DataTable data;
            string query = "select * from DoDung";
            data = DataProvider.Instance.ExecuteQuery(query);
            return ConvertDD(data);
        }
        public bool LuuDanhSachSuDung(List<DanhSachDVDD> ds, int mahd)
        {
            try
            {
                foreach(DanhSachDVDD item in ds)
                {
                    if (item.Check == true)
                    {
                        // viết proc lưu xuống hóa đơn đồ dùng
                        string query = "";
                        DataProvider.Instance.ExecuteQuery(query);
                    }
                    else
                    {
                        // viêt proc lưu xuống hóa đơn dịch vụ
                        string query = "";
                        DataProvider.Instance.ExecuteQuery(query);
                    }
                }
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}