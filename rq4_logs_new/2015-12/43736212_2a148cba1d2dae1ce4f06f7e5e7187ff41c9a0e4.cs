using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QuanLyThuChi.Model;

namespace QuanLyThuChi.DAL
{
    class DAL_GhiChep
    {
        SQLite.Net.SQLiteConnection conn = (new ConnectToDB()).getConnect();
        public List<Model_HANG_MUC_CHI> GetHangMucChi()
        {
            var query = conn.Table<Model_HANG_MUC_CHI>();
            List<Model_HANG_MUC_CHI> listHangMucChi = new List<Model_HANG_MUC_CHI>();
            foreach(var item in query)
            {
                listHangMucChi.Add(new Model_HANG_MUC_CHI() {
                    IdHangMucChiCha = item.IdHangMucChiCha,
                    IdHangMucChi = item.IdHangMucChi,
                    TenHangMucChi = item.TenHangMucChi,
                    ChinhSuaHangMucChi = item.ChinhSuaHangMucChi,
                    GhiChuHangMucChi = item.GhiChuHangMucChi });
            }
            return listHangMucChi;
        }
    }
}