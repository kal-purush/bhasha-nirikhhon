using System;
using System.Collections.Generic;
using System.Text;

namespace chapter_08.domain.service._667
{
    class boy667
    {
        public const  int final_MIN_AGE = 0;
        public const int final_MAX_AGE = 100;
        public string name;
        public int age;
        public string Name
        {
            get { return name; }
            set { name = value; }
        }
        public int Age
        {
            get { return age; }
            set { age = value; }
        }
    }
}