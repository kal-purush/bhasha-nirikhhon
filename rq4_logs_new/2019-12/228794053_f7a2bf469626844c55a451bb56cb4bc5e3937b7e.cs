using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GestionNote.Classes.Data
{
    class generateUsers
    {
        public void GenerateUsers()
        {
            using (AppContext context = new AppContext())
            {
                Student student = new Student("eleve", "eleve", "eleve",18,"B3");
                Teacher teacher = new Teacher("teacher", "teacher", "teacher");

                context.GetStudents.Add(student);
                context.GetTeachers.Add(teacher);

                context.SaveChanges();
            }
        }
    }
}