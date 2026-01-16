using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SendIt.models
{

        public class Pengiriman
        {
            public int Id { get; set; }
            public string Nama { get; set; }
            public int Berat { get; set; }
            public string AlamatTujuan { get; set; }
            public string AlamatJemput { get; set; }
            public int Jarak { get; set; }
            public string NomorTelepon { get; set; }
            public string MetodePembayaran { get; set; }
            public int Harga { get; set; }
            public int IdKurir { get; set; }    
            public int IdPengirim { get; set; }
        public string Status { get; set; }
        }
}