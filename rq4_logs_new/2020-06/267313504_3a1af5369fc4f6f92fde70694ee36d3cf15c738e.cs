using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace FAMS_AngularApi.Models.BrokerMaster
{
    public class BindAllFields2
    {
        public Nullable<Int64> srNo { get; set; }
        public Nullable<Int64> BMId { get; set; }
        public string BrokerName { get; set; }
        public string TradeName { get; set; }
        public string RegistrationNo { get; set; }
        public string GSTNo { get; set; }
        public string StockExchangeName { get; set; }
        public string Email { get; set; }
        public string Telephone { get; set; }
        public string ContactEmail { get; set; }
        public string Phone { get; set; }
        public string Extension { get; set; }
        public string MobileNo { get; set; }
        public string ContactPerson { get; set; }
    }
}