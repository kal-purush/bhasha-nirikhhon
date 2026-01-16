using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BMO.GameDevUnity.CSharp1.Pract6
{
    class Student
    {
        public string lastName;
        public string firstName;
        public string university;
        public string faculty;
        public int course;
        public string department;
        public int group;
        public string city;
        public int age;
        // Создаем конструктор
        public Student(string firstName, string lastName, string university, string faculty, string department, int age, int course, int group, string city)
        {
            this.lastName = lastName;
            this.firstName = firstName;
            this.university = university;
            this.faculty = faculty;
            this.department = department;
            this.course = course;
            this.age = age;
            this.group = group;
            this.city = city;
        }

        public Student()
        {
            this.lastName = " ";
            this.firstName = " ";
            this.university = " ";
            this.faculty = " ";
            this.department = " ";
            this.course = 0;
            this.age = 0;
            this.group = 0;
            this.city = " ";
        }

        public void Copy(Student student)
        {
            this.lastName = student.lastName;
            this.firstName = student.firstName;
            this.university = student.university;
            this.faculty = student.faculty;
            this.department = student.department;
            this.course = student.course;
            this.age = student.age;
            this.group = student.group;
            this.city = student.city;
        }

        public static void SortByAge(ref List<Student> students)
        {
            Student buffer = new Student();
            for (int i = 0; i < students.Count; i++)
            {
                for (int j = 0; j < students.Count - 1; j++)
                {
                    if (students[j].age > students[j + 1].age)
                    {
                        buffer.Copy(students[j]);
                        students[j].Copy(students[j + 1]);
                        students[j + 1].Copy(buffer);
                    }
                }
            }
        }

        public static void SortByCourse(ref List<Student> students)
        {
            Student buffer = new Student();
            for (int i = 0; i < students.Count; i++)
            {
                for (int j = 0; j < students.Count - 1; j++)
                {
                    if (students[j].course > students[j + 1].course)
                    {
                        buffer.Copy(students[j]);
                        students[j].Copy(students[j + 1]);
                        students[j + 1].Copy(buffer);
                    }
                }
            }
        }

        public static void SortByCourseAndAge(ref List<Student> students)
        {
            Dictionary<int, List<Student>> bufferList= new Dictionary<int, List<Student>>()
            {
                {1, new List<Student>() },
                {2, new List<Student>() },
                {3, new List<Student>() },
                {4, new List<Student>() },
                {5, new List<Student>() },
                {6, new List<Student>() },
            };
            Student buffer = new Student();
            for (int i = 1; i < 7; i++)
            {
                foreach (var student in students)
                {
                    if (student.course == i)
                    {
                        bufferList[i].Add(student);
                    }
                }
                for (int j = 0; j < bufferList[i].Count; j++)
                {
                    for (int g = 0; g < bufferList[i].Count - 1; g++)
                    {
                        if (bufferList[i][g].age > bufferList[i][g + 1].age)
                        {
                            buffer.Copy(bufferList[i][g]);
                            bufferList[i][g].Copy(bufferList[i][g + 1]);
                            bufferList[i][g + 1].Copy(buffer);
                        }
                    }
                }
            }
            students.Clear();
            for (int i = 1; i < 7; i++)
            {
                students.AddRange(bufferList[i]);
            }                
        }

    }
}