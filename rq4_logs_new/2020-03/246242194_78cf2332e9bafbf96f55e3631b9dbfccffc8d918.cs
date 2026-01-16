using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TTN_QL_HSGV.DTO
{
    class GiangDay
    {
        private int MaGV;
        private int MaLop;
        private int SoTiet;

        public GiangDay(int maGV, int maLop, int soTiet)
        {
            MaGV = maGV;
            MaLop = maLop;
            SoTiet = soTiet;
        }

        public int MaGV1 { get => MaGV; set => MaGV = value; }
        public int MaLop1 { get => MaLop; set => MaLop = value; }
        public int SoTiet1 { get => SoTiet; set => SoTiet = value; }
    }
}