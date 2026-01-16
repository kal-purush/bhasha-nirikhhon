using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LJGHistoryService.Tables
{
    public class Contract : TableEntity
    {

        public int Id { get; set; }
        public string CompanyName { get; set; }
        public string Location { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public string TypeOfEmployment { get; set; }

        public Contract(string companyName, string location, DateTime startDate, DateTime endDate, string typeOfEmployment)
        {
            CompanyName = companyName;
            Location = location;
            StartDate = startDate;
            EndDate = endDate;
            TypeOfEmployment = typeOfEmployment;
        }


        public Contract()
        {

        }
    }
}