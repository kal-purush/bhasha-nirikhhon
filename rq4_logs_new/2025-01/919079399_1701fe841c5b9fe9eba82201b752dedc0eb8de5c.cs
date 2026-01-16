using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models.DTOs
{
    public class FutureOpenPositionsResponseDto
    {
        public string Symbol { get; set; }

        public string PositionSide { get; set; }
            
        public float EntryPrice { get; set; }

        public float UnRealizedProfit { get; set; }

        public float LiquidationPrice { get; set; }

        public float Notional { get; set; }
    }
}