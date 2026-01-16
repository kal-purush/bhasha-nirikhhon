using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CLasswork_6
{
    public class Course
    {
        public string Title { get; set; }
        public int  Price { get; set; }
        public Student1[] Students { get; set; }

        public Course(string title, int price, Student1[] students)
        {
            Title = title;
            Price = price;
            Students = students;
        }


        public void ShowInfo()
        {
            Console.WriteLine("Course " + Title +"Price "+ Price);
            ShowStudents();

        }
        public void ShowStudents()
        {
            foreach (Student1 student in Students)
            {
             student.ShowInfo();
            }

        }
    }
}