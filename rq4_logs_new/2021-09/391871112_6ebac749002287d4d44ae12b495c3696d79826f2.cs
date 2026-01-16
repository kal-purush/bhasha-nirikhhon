using System;
namespace ConasiCRM.Portable.Models
{
    public class QuoteUnitInforModel
    {
        public Guid productid { get; set; }
        public string name { get; set; }
        public Guid _bsd_projectcode_value { get; set; }
        public Guid _bsd_phaseslaunchid_value { get; set; }
        public Guid _bsd_unittype_value { get; set; }

        public string statuscode { get; set; }
        public decimal bsd_constructionarea { get; set; }
        public decimal bsd_netsaleablearea { get; set; }
        public decimal bsd_actualarea { get; set; }

        public string project_name { get; set; }
        public string phaseslaunch_name { get; set; }
        public string pricelist_name { get; set; }

        public decimal bsd_taxpercent { get; set; }
        public decimal bsd_queuingfee { get; set; }
        public decimal bsd_depositamount { get; set; }
    }
}