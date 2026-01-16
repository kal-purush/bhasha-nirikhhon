using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace jaytwo.TimeZones.CodeGen.Cldr
{
    public class CldrTimeZoneKey
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public IList<string> Aliases { get; set; }
        public bool IsDeprecated { get; set; }
		public string Preferred { get; set; }
    }
}