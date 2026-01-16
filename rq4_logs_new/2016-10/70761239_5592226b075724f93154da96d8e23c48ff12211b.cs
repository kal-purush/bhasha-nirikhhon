using RepoQuiz.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace RepoQuiz.DAL
{
    public class StudentRepository
    {
        public StudentContext Context { get; set; }

        public StudentRepository()
        {
            Context = new StudentContext();
        }

        public StudentRepository(StudentContext _context)
        {
            Context = _context;
        }

        public List<Student> GetStudents()
        {
            int i = 1;
            return Context.Students.ToList();
        }

        public void AddStudents(Student student)
        {
            Context.Students.Add(student);
            Context.SaveChanges();
        }

        public void AddStudent(string FirstName, string lastName, string major)
        {
            Student student = new Student { FirstName = firstName, LastName = lastName, Major = major };
            Context.Students.Add(student);
            Context.SaveChanges();
        }

        public Student FindStudentById(int id)
        {
            Student found_student = Context.Students.FirstOrDefault(a => a.StudentID == id);
            return found_student;
        }

       
    }
}