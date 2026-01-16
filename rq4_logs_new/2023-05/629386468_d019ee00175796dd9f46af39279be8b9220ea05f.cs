using hogward;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Documents;
using System.IO;

namespace hogward
{
    public class FileName
    {
        public static void Test()
        {
            var list = JsonConvert.DeserializeObject<AuthorizePersons[]>(File.ReadAllText("JSON_DATA.json"));
            int Cprofessor = 0;
            for (int i = 0; i < list.Length; i++)
            {
                if (list[i].role == "teacher")
                {
                    Cprofessor++;
                }

            }
            int Cstudent = list.Length - Cprofessor;
            List<Professor> professors = new List<Professor>();
            List<Student> students = new List<Student>();
            int CCprofessor = 0;
            int CCstudent = 0;
            for (int i = 0; i < list.Length; i++)
            {
                if (list[i].role == "teacher")
                {
                    Professor professor = new Professor();
                    professor.Name = list[i].Name;
                    professor.Family = list[i].Family;
                    professor.DateOfBirth = list[i].DateOfBirth;
                    professor.Father = list[i].Father;
                    professor.Username = list[i].Username;
                    professor.Password = list[i].Password;
                    professor.Type = list[i].Type;
                    professor.Gender = list[i].Gender;
                    professor.role = list[i].role;
                    //professors[CCprofessor].baggage = list[i].baggage;
                    professor.pet = list[i].pet;
                    professor.massage = list[i].massage;
                    //professors[CCprofessor].group = list[i].group;
                    professor.curriculum = list[i].curriculum;
                    professor.room = list[i].room;
                    professors.Add(professor);
                    Cprofessor++;
                }
                else
                {
                    Student student = new Student();
                    student.Name = list[i].Name;
                    student.Family = list[i].Family;
                    student.DateOfBirth = list[i].DateOfBirth;
                    student.Father = list[i].Father;
                    student.Username = list[i].Username;
                    student.Password = list[i].Password;
                    student.Type = list[i].Type;
                    student.Gender = list[i].Gender;
                    student.role = list[i].role;
                    student.baggage = list[i].baggage;
                    student.pet = list[i].pet;
                    student.massage = list[i].massage;
                    student.group = list[i].group;
                    student.curriculum = list[i].curriculum;
                    student.room = list[i].room;
                    students.Add(student);
                    Cstudent++;
                }
            }
            string json = JsonConvert.SerializeObject(professors);
            File.WriteAllText("Professors.json", json);
            string Json = JsonConvert.SerializeObject(students);
            File.WriteAllText("Students.json", Json);
        }
    }
}
