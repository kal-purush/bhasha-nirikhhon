using System;
using System.Collections.Generic;
using System.Linq;


namespace Gradebook
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Please enter 5 student names.");
            string[] students = new string[5];
            for (int i = 0; i < students.Length; i++) {
                Console.WriteLine("Student " +(i+1)+ ": ");
                students[i] = Console.ReadLine();
            }

            Console.WriteLine("Now, enter 3 grades for each student.");
            string[][] grades = new string[5][];
            for (int i = 0; i < grades.Length; i++) {
                Console.WriteLine("Student " +(i+1)+ " grades: ");
                string studentgrades = Console.ReadLine();
                grades[i] = studentgrades.Split(" ");
            }





            Dictionary<string, string[]> categories = new Dictionary<string, string[]>();
            for (int i = 0; i < students.Length; i++) {
                    categories.Add(students[i], grades[i]);
            }

            foreach (var key in categories.Keys)
            {
                 Console.WriteLine("Student " +key);
                foreach (var grade in categories[key])
		        {
			        Console.WriteLine(grade);
		        }
            }



//             Console.WriteLine("Student 1's highest grade is: " +student1Array.Max());
//             Console.WriteLine("Student 1's lowest grade is: " +student1Array.Min());
//             var sum = student1Array.Sum(grades=>Convert.ToInt32(grades));
//             int avg = sum / 3;
//             Console.WriteLine("The average is: " +avg);
//             Console.WriteLine("Student 2: " +student[1]);
//             foreach (var grade in categories["student2"])
// 		    {
// 			    Console.WriteLine(grade);
// 		    }
//              Console.WriteLine("Student 2's highest grade is: " +student2Array.Max());
//             Console.WriteLine("Student 2's lowest grade is: " +student2Array.Min());
//             var sum2 = student2Array.Sum(grades2=>Convert.ToInt32(grades2));
//             int avg2 = sum2 / 3;
//             Console.WriteLine("The average is: " +avg2);
//             Console.WriteLine("Student 3: " +student3);
//             foreach (var grade in categories["student3"])
// 		    {
// 			    Console.WriteLine(grade);
// 		    }
//              Console.WriteLine("Student 3's highest grade is: " +student3Array.Max());
//             Console.WriteLine("Student 3's lowest grade is: " +student3Array.Min());
//             var sum3 = student3Array.Sum(grade3=>Convert.ToInt32(grade3));
//             int avg3 = sum3 / 3;
//             Console.WriteLine("The average is: " +avg3);
//             Console.WriteLine("Student 4: " +student4);
//             foreach (var grade in categories["student4"])
// 		    {
// 			    Console.WriteLine(grade);
// 		    }
//              Console.WriteLine("Student 4's highest grade is: " +student4Array.Max());
//             Console.WriteLine("Student 4's lowest grade is: " +student4Array.Min());
//             var sum4 = student1Array.Sum(grades4=>Convert.ToInt32(grades4));
//             int avg4 = sum4 / 3;
//             Console.WriteLine("The average is: " +avg4);
//             Console.WriteLine("Student 5: " +student5);
//             foreach (var grade in categories["student5"])
// 		    {
// 			    Console.WriteLine(grade);
// 		    }
//              Console.WriteLine("Student 5's highest grade is: " +student5Array.Max());
//             Console.WriteLine("Student 5's lowest grade is: " +student5Array.Min());
//             var sum5 = student5Array.Sum(grades5=>Convert.ToInt32(grades5));
//             int avg5 = sum5 / 3;
//             Console.WriteLine("The average is: " +avg5);
        }
    }
}