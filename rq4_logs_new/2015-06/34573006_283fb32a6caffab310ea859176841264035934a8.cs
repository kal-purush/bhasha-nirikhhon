using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace modbrowser
{
    public class Mod
    {
        public class Jar
        {
            public string url { get; set; }
            public string path { get; set; }
        }

        public string name { get; set; }
        public string author { get; set; }
        public string version { get; set; }
        public string description { get; set; }
        public string image_url { get; set; }
        public Jar jar { get; set; }
    }
}