using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TeamVoldemort.Store
{
    class Store
    {
        public int StoreNumber { get; set; }
        public string GasYearly { get; set; }
        public string GasCurrentQuarter { get; set; }
        public string RetailYearly { get; set; }
        public string RetailCurrentQuarter { get; set; }
        public  List<string> EmployeeList { get; set; }

        public Store(int storeNumber, string gasYearly,string gasCurrentQuarter, string retailYearly, string retailCurrentQuarter, List<string> employeeList)
        {
            StoreNumber = storeNumber;
            GasYearly = gasYearly;
            GasCurrentQuarter = gasCurrentQuarter;
            RetailYearly = retailYearly;
            RetailCurrentQuarter = retailCurrentQuarter;
            EmployeeList = employeeList;
        }
    }
}