using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace home_lesson_2_7
{
   
    enum Subject { prog, admin, design };
    
    class Program
    {
        static void Main(string[] args)
        {
            Student st1 = new Student("Ivan", "Ivanovich", "Ivanov");
            st1.Group = "3M-333";
            st1.Age = 25;
            int[] arr = new int[10] {7,8,7,8,7,8,7,8,7,8};
            int[] arr1 = new int[12] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };


            Subject pr = Subject.prog;
            Subject adm = Subject.admin;
            Subject des = Subject.design;

           
            st1.AddMark(11, 2);
            st1.SetMarksArray(ref arr, 0);
            st1.SetMarksArray(ref arr1, 1);
           
            st1.print();
        }
    }
}