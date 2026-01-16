using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GoLittleRockstar.Model
{
    [PrimaryKey(nameof(id))]
    public class clsSehir
    {
        public int id { get; set; }
        public string SehirAdi { get; set; }    
        public decimal? lat { get; set; }
        public decimal? lon { get; set; }
    }
}