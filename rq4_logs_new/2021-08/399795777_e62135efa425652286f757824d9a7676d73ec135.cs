using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LGAConnectSOMS.Models
{
    public class Students
    {
        //public int ID { get; set; }
        public string Lastname { get; set; }
        public string Firstname { get; set; }
        public string Middlename { get; set; }
        //public string Gender { get; set; }
        [DisplayName("Grade Level")]
        public string Grade_Level  { get; set; }
        public string Section { get; set; }
    }
}