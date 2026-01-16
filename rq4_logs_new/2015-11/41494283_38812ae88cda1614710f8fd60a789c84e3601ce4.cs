using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SigaaWindows.Model.API
{
    public class TaskDetailsResult
    {
        public string id { get; set; }
        public string title { get; set; }
        public string course_title { get; set; }
        public string opened_date { get; set; }
        public string delivery_date { get; set; }
        public string description { get; set; }
        public string attachment { get; set; }
        public string answers { get; set; }
    }
}