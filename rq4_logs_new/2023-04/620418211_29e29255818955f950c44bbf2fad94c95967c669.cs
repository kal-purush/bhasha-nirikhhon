using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Home_5
{
    public class Groups
    {
        public Student[] Group1 { get; set; }
        public Student[] Group2 { get; set; }
        public Student[] Group3 { get; set; }

        public Groups(Student[] group1, Student[] group2, Student[] group3)
        {
            Group1 = group1;
            Group2 = group2;
            Group3 = group3;
        }
    }


}