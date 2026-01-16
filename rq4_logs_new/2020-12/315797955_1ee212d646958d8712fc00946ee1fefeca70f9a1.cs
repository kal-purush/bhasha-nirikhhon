using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Pet_Pillbox.Models
{
    public class LastDoseLog
    {
        public string Name { get; set; }
        public int HoursBetweenDoses { get; set; }
        public DateTime LastDoseDateTime { get; set; }
    }
}