using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpReview
{
    class Program 
    {
        static void Main(string[] args)
        {
            // Create an array of 5 students
            Student[] students = new Student[5];

            // Create a for loop to enter data 5 times
            for(int i = 0; i < 5; i++){

                // Create instance of students
                students[i] = new Student();

                Console.WriteLine("Enter Student " +(i+1)+ "  first name: ");
                students[i].fName = Console.ReadLine();
                Console.WriteLine("Enter Student " + (i+1) + "  last name: ");
                students[i].lName = Console.ReadLine();
                Console.WriteLine("Enter Student " + (i + 1) + "  address: ");
                students[i].address = Console.ReadLine();
                Console.WriteLine("Enter Student " + (i + 1) + "  state: ");
                students[i].state = Console.ReadLine();
                Console.WriteLine("Enter Student " + (i + 1) + "  age: ");
                students[i].age = Console.ReadLine();
                Console.WriteLine("Enter Student " + (i + 1) + "  zip code: ");
                students[i].zipCode = Console.ReadLine();
                Console.WriteLine("Enter Student " + (i + 1) + "  visa status: ");
                students[i].visa = Console.ReadLine();

                // Display results
                students[i].DisplayInfo();
            }

            Console.ReadKey();
            
        }
    }
}