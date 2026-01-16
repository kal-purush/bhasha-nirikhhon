using System;
using System.Collections.Generic;
using System.Linq;

namespace ConasiCRM.Portable.Models
{
    public class ContractTypeData
    {
        public static List<OptionSet> ContractTypes()
        {
            return new List<OptionSet>()
            {
                new OptionSet("100000000","Long term lease"),
                new OptionSet("100000001","Local SPA"),
                new OptionSet("100000002","Foreigner SPA"),
            };
        }
        public static OptionSet GetContractTypeById(string id)
        {
            return ContractTypes().SingleOrDefault(x => x.Val == id);
        }
    }
}