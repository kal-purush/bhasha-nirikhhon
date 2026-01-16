using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace TTN_QL_HSGV.DTO
{
    class GiangDayComparer : IComparer<GiangDay>
    {
        private string memberName = string.Empty;
        private SortOrder sortOrder = SortOrder.None;

        public GiangDayComparer(string memberName, SortOrder sortOrder)
        {
            this.memberName = memberName;
            this.sortOrder = sortOrder;
        }
        public int Compare(GiangDay x, GiangDay y)
        {
            switch (memberName)
            {
                case "MaLop":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.MaLop.CompareTo(y.MaLop);
                        }
                        else
                        {
                            return y.TenHS.CompareTo(x.MaLop);
                        }
                    }
                case "SoTiet":
                    {
                        if (sortOrder == SortOrder.Ascending)
                        {
                            return x.SoTiet.CompareTo(y.SoTiet);
                        }
                        else
                        {
                            return y.SoTiet.CompareTo(x.SoTiet);
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
                            return y.MaGV.CompareTo(x.MaGV);
                        }
                    }
            }
    }
}