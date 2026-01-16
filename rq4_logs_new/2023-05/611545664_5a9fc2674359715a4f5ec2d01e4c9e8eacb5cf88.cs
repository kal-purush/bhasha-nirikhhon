using MarketDataAnalyser.Code.ClassPub;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EveMarketDataAnalyser.Code.ClassPub
{
    public class DoctrineItem : MarketItem
    {
        public long SeuilFit { get; set; }
        
        public DoctrineItem(MarketItem marketitem, long seuilFit) : base(marketitem)
        {
            SeuilFit = seuilFit;
        }



    }
}