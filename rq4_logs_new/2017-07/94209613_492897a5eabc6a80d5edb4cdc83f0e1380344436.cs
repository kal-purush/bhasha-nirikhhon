using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Screen2.Entity
{
    public class AsxEod : BaseEntity
    {
        public string Symbol { get; set; }
        public int TradingDate { get; set; }
        public double Open { get; set; }
        public double Close { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public long Volumn { get; set; }
        public double? AdjustedClose { get; set; }

        [Required]
        [Index]
        public int ShareId { get; set; }

        public virtual Share Share { get; set; }
    }
}