using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ColdPlayProject.area_of_emphasis
{
    public class ClassRoomOne
    {
        private string[] _students;

        public ClassRoomOne()
        {
            this._students = new string[7];

            _students[0] = "Sam";
            _students[1] = "Taitai";
            _students[2] = "Deji";
            _students[3] = "Casi";
            _students[4] = "Joy";
            _students[5] = "Andre";
            _students[6] = "Dotcom";
        }


        public string SearchForStudent(string name)
        {
            foreach (var student in _students)
            {
                if (student.Contains(name))
                {
                    return "Yes I have seen the student and he is in the class";
                }
            }
            return "There is no such student in the class";
        }


    }

    public class ClassRoomTwo
    {
    }

    public class ClassRoomThree
    {
    }

    public class ClassRoomFour
    {
    }

}