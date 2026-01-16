using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace FAMS_AngularApi.Models.PMSEmployees
{
    public class BindAllCustomersFields
    {
        public Nullable<Int64> srNo { get; set;}
        public string CustomerName { get; set; }
        public string CustomerCode { get; set;}
        public Nullable<DateTime> EmpLinkingDate {get; set;}
        public Nullable<DateTime> InceptionDate { get; set;}
        public string Custodian { get; set;}
        public Nullable<Int64> CustId { get; set;}
    }
}