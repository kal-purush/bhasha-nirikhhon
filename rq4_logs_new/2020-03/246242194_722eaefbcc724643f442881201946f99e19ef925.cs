using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace TTN_QL_HSGV.DTO
{
    class GiaoVienComparer : IComparer<GiaoVien>
    {

        private string memberName = string.Empty;
        private SortOrder sortOrder = SortOrder.None;

        public GiaoVienComparer(string memberName, SortOrder sortOrder)
        {
            this.memberName = memberName;
            this.sortOrder = sortOrder;
        }

        public int Compare(GiaoVien x, GiaoVien y)
        {
            switch(memberName)
            {
                case "TenGV":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.TenGV.CompareTo(y.TenGV);
                        }
                        else
                        {
                            return y.TenGV.CompareTo(x.TenGV);
                        }
                    }
                case "DiaChi":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.DiaChi.CompareTo(y.DiaChi);
                        }
                        else
                        {
                            return y.DiaChi.CompareTo(x.DiaChi);
                        }
                    }
                case "GioiTinh":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.GioiTinh.CompareTo(y.GioiTinh);
                        }
                        else
                        {
                            return y.GioiTinh.CompareTo(x.GioiTinh);
                        }
                    }
                case "SDT":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.SDT.CompareTo(y.SDT);
                        }
                        else
                        {
                            return y.SDT.CompareTo(x.SDT);
                        }
                    }
                case "ChucVu":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.ChucVu.CompareTo(y.ChucVu);
                        }
                        else
                        {
                            return y.ChucVu.CompareTo(x.ChucVu);
                        }
                    }
                case "MaMon":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.MaMon.CompareTo(y.MaMon);
                        }
                        else
                        {
                            return y.MaMon.CompareTo(x.MaMon);
                        }
                    }
                default:
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.MaGV.CompareTo(y.MaGV);
                        }
                        else
                        {
                            return y.MaMaGV.CompareTo(x.MaGV);
                        }
                    }
            }
        }
    }
}