using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TTN_QL_HSGV.DTO
{
    class Lop
    {
        private int MaLop;
        private string TenLop;
        private int MaGVCN;
        private int MaKhoaHoc;

        public Lop(int maLop, string tenLop, int maGVCN, int maKhoaHoc)
        {
            MaLop = maLop;
            TenLop = tenLop;
            MaGVCN = maGVCN;
            MaKhoaHoc = maKhoaHoc;
        }

        public int MaLop1 { get => MaLop; set => MaLop = value; }
        public string TenLop1 { get => TenLop; set => TenLop = value; }
        public int MaGVCN1 { get => MaGVCN; set => MaGVCN = value; }
        public int MaKhoaHoc1 { get => MaKhoaHoc; set => MaKhoaHoc = value; }
    }
}