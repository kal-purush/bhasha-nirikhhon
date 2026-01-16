using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TODA.Model
{
    public class Class
    {
        public string Name { get; set; }
        public string Proffession { get; set; }
        public Student[] Students { get; set; }
    }

    public class ClassWithIDs
    {
        public string Name { get; set; }
        public string Proffession { get; set; }
        public string[] Students { get; set; }
    }
}