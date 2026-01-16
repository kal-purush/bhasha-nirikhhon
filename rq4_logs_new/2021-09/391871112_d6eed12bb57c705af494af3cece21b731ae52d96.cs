using System;
using System.Collections.Generic;
using System.Linq;

namespace ConasiCRM.Portable.Models
{
    public class CaseTypeData
    {
        public static List<OptionSet> CasesData()
        {
            return new List<OptionSet>()
            {
                new OptionSet("1","Question"),
                new OptionSet("2","Problem"),
                new OptionSet("3","Request"),
            };
        }

        public static OptionSet GetCaseById(string id)
        {
            return CasesData().SingleOrDefault(x => x.Val == id);
        }
    }
}