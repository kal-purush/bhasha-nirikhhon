using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SupplyChainManagement.Models.Xml
{
    public class InwardStockOrder
    {
        public int Id;
        public int OrderPeriod;
        public int Mode;
        public int Article;
        public int Amount;
        public int Time;
        public double MaterialCosts;
        public double OrderCosts;
        public double EntireCosts;
        public double PieceCosts;
    }
}