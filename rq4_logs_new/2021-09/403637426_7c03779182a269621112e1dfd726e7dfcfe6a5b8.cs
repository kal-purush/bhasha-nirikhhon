using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassRoom
{
    class Student
    {
        private string _name;
        private int _birthMonth;
        private int _birthday;

        public Student(string name, int birthMonth, int birthday)
        {
            _name = name;
            _birthMonth = birthMonth;
            _birthday = birthday;
        }

        public string Name
        {
            get { return _name; }
            //set { _name = value; }
        }

        public int BirthMonth
        {
            get { return _birthMonth; }
            //set { _birthMonth = value; }
        }

        public int Birthday
        {
            get { return _birthday; }
            //set { _birthday = value; }
        }

    }
}