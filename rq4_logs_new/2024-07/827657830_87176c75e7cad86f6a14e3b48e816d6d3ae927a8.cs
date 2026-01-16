using System;
using DAL;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DTO.Models;

namespace BLL
{
    public class ThucDonBLL
    {
        ThucDonDAL thucDonDAL = new ThucDonDAL();
        Monan monan = new Monan();

        public List<Monan> getallmonan()
        {
            return thucDonDAL.getallmonan().ToList();
        }
        public string add(int id,string ten, decimal gia, string loai, string trangthai)
        {
            Monan monan = new Monan()
            {
                IdMonAn = id,
                TenMon = ten,
                DonGia = gia,
                Loai = loai,
                TrangThai = trangthai
            }; if (thucDonDAL.addmonan(monan))
            {
                return "Them thanh cong";

            }
            else return "Them that bai";
        }
        public string update(int id, string ten, decimal gia, string loai, string trangthai)
        {
            Monan monan = new Monan()
            {
                IdMonAn = id,
                TenMon = ten,
                DonGia = gia,
                Loai = loai,
                TrangThai = trangthai
            }; if (thucDonDAL.updateMonAn(monan))
            {
                return "Sua thanh cong";

            }
            else return "Sua that bai";
        }
        public string delete(int id)
        {

            if (thucDonDAL.deleteMonan(id))
            {
                return "Xoa thanh cong";

            }
            else return "Xoa that bai";
        }

    }
}