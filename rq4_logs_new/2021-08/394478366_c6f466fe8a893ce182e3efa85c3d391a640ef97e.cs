using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TeamVoldemort.District
{
    class District
    {
        public string DistrictManager { get; set; }
        public string Name { get; set; }
        public List<string> StoreList { get; set; }

        public District(string districtManager, string name, List<string> storeList)
        {
            DistrictManager = districtManager;
            Name = name;
            StoreList = storeList;
        }
    }

}