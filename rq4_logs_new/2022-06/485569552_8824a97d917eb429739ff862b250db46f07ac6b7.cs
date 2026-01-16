using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Siapel.Domain.Models
{
    public class InOutStok
    {
        public int Id { get; set; }
        public DateTimeOffset Tanggal { get; set; }
        public string Item { get; set; }
        public int StokAwal { get; set; }
        public int Masuk { get; set; }
        public int Penjualan { get; set; }
        public int TitipanBocor { get; set; }
        public int AmbilBocor { get; set; }
    }
}