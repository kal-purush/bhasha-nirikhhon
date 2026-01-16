using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BMO.GameDevUnity.CSharp1.Pract5
{
    class Student
    {
        private string lastName;
        private string firstName;
        private int[] ratings = new int[3];
        private double averageRating;

        public string LastName
        {
            get
            {
                return lastName;
            }
        }

        public string FirstName
        {
            get
            {
                return firstName;
            }
        }

        public double AverageRating
        {
            get
            {
                return (double)(ratings[0] + ratings[1] + ratings[2]) / 3;
            }
        }

        public Student(string lastName, string firstName, int firstRating, int secondRating, int thirdRating)
        {
            this.lastName = lastName;
            this.firstName = firstName;
            this.ratings[0] = firstRating;
            this.ratings[1] = secondRating;
            this.ratings[2] = thirdRating;
            this.averageRating = AverageRating;
        }

        public Student(string data)
        {
            string[] dataSplit = data.Split(' ');
            this.lastName = dataSplit[0];
            this.firstName = dataSplit[1];
            for (int i = 2; i < 5; i++)
            {
                this.ratings[i - 2] = int.Parse(dataSplit[i]);
            }
            this.averageRating = AverageRating;
        }

        public Student()
        {
            this.lastName = " ";
            this.firstName = " ";
            for (int i = 0; i < 3; i++)
            {
                this.ratings[i] = 0;
            }
        }

        public static void Copy(Student student, ref Student copy)
        {
            copy.lastName = student.lastName;
            copy.firstName = student.firstName;
            for (int i = 0; i < student.ratings.Length; i++)
            {
                copy.ratings[i] = student.ratings[i];
            }
            copy.averageRating = copy.AverageRating;
        }

        public void Copy(Student student)
        {
            this.lastName = student.lastName;
            this.firstName = student.firstName;
            for (int i = 0; i < student.ratings.Length; i++)
            {
                this.ratings[i] = student.ratings[i];
            }
            this.averageRating = AverageRating;
        }

        private static void SortAverageRating(ref Student[] students)
        {
            Student buffer = new Student();
            for (int i = 0; i < students.Length; i++)
            {
                for (int j = 0; j < students.Length-1; j++)
                {
                    if (students[j].averageRating > students[j+1].averageRating)
                    {
                        buffer.Copy(students[j]);
                        students[j].Copy(students[j+1]);
                        students[j+1].Copy(buffer);
                    }
                }
            }
        }

        public static Student[] WorstStudents(Student[] students)
        {
            int i = 0;
            int j = 1;
            Student[] worstStudents = new Student[3];
            Student[] studentsSort = new Student[students.Length];
            students.CopyTo(studentsSort, 0);
            Student.SortAverageRating(ref studentsSort);
            for (int count = 0; count < 3; count++)
            {
                worstStudents[i] = studentsSort[i];
                while (true)
                {
                    if (studentsSort[i].averageRating != studentsSort[i + j].averageRating)
                    {
                        break;
                    }
                    Array.Resize(ref worstStudents, worstStudents.Length + 1);
                    worstStudents[i + j] = studentsSort[i + j];
                    j++;
                }
                i = i + j; j = 1;
            }
            return worstStudents;
        }
    }
}