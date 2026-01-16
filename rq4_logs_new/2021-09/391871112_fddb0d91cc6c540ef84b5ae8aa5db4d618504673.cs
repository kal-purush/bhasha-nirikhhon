using System;
using System.Collections.Generic;
using System.Linq;

namespace ConasiCRM.Portable.Models
{
    public class NetAreaDirectSaleData
    {
        public static List<OptionSet> NetAreaData()
        {
            return new List<OptionSet>()
            {
                new OptionSet("1","Dưới 60m2"),
                new OptionSet("2","60m2 -> 80m2"),
                new OptionSet("3","81m2 -> 100m2"),
                new OptionSet("4","101m2 -> 120m2"),
                new OptionSet("5","121m2 -> 150m2"),
                new OptionSet("6","151m2 -> 180m2"),
                new OptionSet("7","211m2 -> 240m2"),
                new OptionSet("8","241m2 -> 270m2"),
                new OptionSet("9","271m2 -> 300m2"),
                new OptionSet("10","301m2 -> 350m2"),
                new OptionSet("11","Trên 350m2"),
            };
        }
        public static OptionSet GetNetAreaById(string Id)
        {
            return NetAreaData().SingleOrDefault(x => x.Val == Id);
        }
    }
}