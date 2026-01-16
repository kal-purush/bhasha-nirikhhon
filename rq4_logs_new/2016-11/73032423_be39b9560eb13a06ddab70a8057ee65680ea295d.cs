using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace YouZhiWenJiao.Web.Common
{
    public class CompanyProfileTypeModel
    {
        public int id { get; set; }
        public int categoryid { get; set; }
        public string description { get; set; }
    }

    public class CompanyProfileModel
    {
        public int id { get; set; }
        public int typeid { get; set; }
        public int categoryid { get; set; }
        public string title { get; set; }
        public string content { get; set; }
        public string picture { get; set; }
        public string contentpicture1 { get; set; }
        public string contentpicture2 { get; set; }
        public string contentpicture3 { get; set; }
        public bool showpicture { get; set; }
        public bool showinhomepage { get; set; }
    }
}