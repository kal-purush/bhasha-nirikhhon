using Entities;

namespace Entities
{
    internal class Course
    {
        public string Name { get; set; }
        public Teacher Teacher { get; set; }
        public HashSet<Student> students { get; set; }

        public Course(string name, Teacher teacher)
        {
            Name = name;
            Teacher = teacher;
            students = new HashSet<Student>(); //Inicia a coleção
        }

        public override string ToString()
        {
            return "Course " + Name + " -\n"  + students.ToString();
        }
    }
}