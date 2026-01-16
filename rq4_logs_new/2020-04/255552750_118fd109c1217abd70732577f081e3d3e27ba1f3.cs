using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuanLyKhachSan
{
    class SuDung
    {
        protected string ma;
        protected string ten;
        protected decimal gia;

        public SuDung()
        {
            this.ma = "";
            this.ten = "";
            this.gia = 0;
        }
        public SuDung(string ma, string ten, decimal gia)
        {
            this.ma = ma;
            this.ten = ten;
            this.gia = gia;
        }

        public string Ma { get => ma; set => ma = value; }
        public string Ten { get => ten; set => ten = value; }
        public decimal Gia { get => gia; set => gia = value; }
    }
}