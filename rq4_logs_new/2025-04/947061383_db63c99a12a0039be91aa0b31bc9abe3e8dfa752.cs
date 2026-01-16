using System;
using System.Collections.Generic;
using System.Linq;

namespace StudentInformationSystem
{
    // Student Class
    public class Student
    {
        public int StudentID { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public DateTime DateOfBirth { get; set; }
        public string Email { get; set; } = string.Empty;
        public string PhoneNumber { get; set; } = string.Empty;

        private List<Enrollment> enrollments = new List<Enrollment>();
        private List<Payment> payments = new List<Payment>();

        public void EnrollInCourse(Course course)
        {
            var enrollment = new Enrollment
            {
                EnrollmentID = new Random().Next(1000, 9999),
                StudentID = this.StudentID,
                CourseID = course.CourseID,
                EnrollmentDate = DateTime.Now
            };
            enrollments.Add(enrollment);
            course.AddEnrollment(enrollment);
        }

        public void UpdateStudentInfo(string firstName, string lastName, DateTime dateOfBirth, string email, string phoneNumber)
        {
            FirstName = firstName;
            LastName = lastName;
            DateOfBirth = dateOfBirth;
            Email = email;
            PhoneNumber = phoneNumber;
        }

        public void MakePayment(decimal amount, DateTime paymentDate)
        {
            payments.Add(new Payment { PaymentID = new Random().Next(1000, 9999), Student = this, Amount = amount, PaymentDate = paymentDate });
        }

        public void DisplayStudentInfo()
        {
            Console.WriteLine($"ID: {StudentID}, Name: {FirstName} {LastName}, DOB: {DateOfBirth.ToShortDateString()}, Email: {Email}, Phone: {PhoneNumber}");
        }

        public List<Enrollment> GetEnrolledCourses() => enrollments;

        public List<Payment> GetPaymentHistory() => payments;
    }

    // Course Class
    public class Course
    {
        public int CourseID { get; set; }
        public string CourseName { get; set; } = string.Empty;
        public string CourseCode { get; set; } = string.Empty;
        public string InstructorName { get; set; } = string.Empty;
        public Teacher AssignedTeacher { get; set; }

        private List<Enrollment> enrollments = new List<Enrollment>();

        public void AssignTeacher(Teacher teacher)
        {
            AssignedTeacher = teacher;
            teacher.AddAssignedCourse(this);
        }

        public void UpdateCourseInfo(string courseCode, string courseName, string instructor)
        {
            CourseCode = courseCode;
            CourseName = courseName;
            InstructorName = instructor;
        }

        public void DisplayCourseInfo()
        {
            Console.WriteLine($"Course ID: {CourseID}, Code: {CourseCode}, Name: {CourseName}, Instructor: {InstructorName}");
        }

        public List<Enrollment> GetEnrollments() => enrollments;

        public Teacher GetTeacher() => AssignedTeacher;

        public void AddEnrollment(Enrollment enrollment)
        {
            enrollments.Add(enrollment);
        }

        public int GetTotalEnrollments() => enrollments.Count;

        public decimal GetTotalPayments(List<Student> students)
        {
            return enrollments.Sum(e => e.GetStudent(students)?.GetPaymentHistory().Sum(p => p.Amount) ?? 0);
        }
    }

    // Enrollment Class
    public class Enrollment
    {
        public int EnrollmentID { get; set; }
        public int StudentID { get; set; }
        public int CourseID { get; set; }
        public DateTime EnrollmentDate { get; set; }

        public Student GetStudent(List<Student> students) => students.FirstOrDefault(s => s.StudentID == StudentID);
        public Course GetCourse(List<Course> courses) => courses.FirstOrDefault(c => c.CourseID == CourseID);
    }

    // Teacher Class
    public class Teacher
    {
        public int TeacherID { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        private List<Course> assignedCourses = new List<Course>();

        public void UpdateTeacherInfo(string firstName, string lastName, string email)
        {
            FirstName = firstName;
            LastName = lastName;
            Email = email;
        }

        public void DisplayTeacherInfo()
        {
            Console.WriteLine($"ID: {TeacherID}, Name: {FirstName} {LastName}, Email: {Email}");
        }

        public List<Course> GetAssignedCourses() => assignedCourses;

        public void AddAssignedCourse(Course course)
        {
            assignedCourses.Add(course);
        }
    }

    // Payment Class
    public class Payment
    {
        public int PaymentID { get; set; }
        public Student Student { get; set; }
        public decimal Amount { get; set; }
        public DateTime PaymentDate { get; set; }

        public Student GetStudent() => Student;
        public decimal GetPaymentAmount() => Amount;
        public DateTime GetPaymentDate() => PaymentDate;
    }

    // SIS Class
    public class SIS
    {
        private List<Student> allStudents = new List<Student>();

        public void EnrollStudentInCourse(Student student, Course course)
        {
            student.EnrollInCourse(course);
            if (!allStudents.Contains(student))
                allStudents.Add(student);
        }

        public void AssignTeacherToCourse(Teacher teacher, Course course)
        {
            course.AssignTeacher(teacher);
        }

        public void RecordPayment(Student student, decimal amount, DateTime paymentDate)
        {
            student.MakePayment(amount, paymentDate);
        }

        public void GenerateEnrollmentReport(Course course)
        {
            Console.WriteLine($"\nEnrollment Report for {course.CourseName}:");
            foreach (var enrollment in course.GetEnrollments())
            {
                Console.WriteLine($"Enrollment ID: {enrollment.EnrollmentID}, Student ID: {enrollment.StudentID}, Date: {enrollment.EnrollmentDate}");
            }
        }

        public void GeneratePaymentReport(Student student)
        {
            Console.WriteLine($"\nPayment Report for {student.FirstName} {student.LastName}:");
            foreach (var payment in student.GetPaymentHistory())
            {
                Console.WriteLine($"Payment ID: {payment.PaymentID}, Amount: {payment.Amount}, Date: {payment.PaymentDate}");
            }
        }

        public void CalculateCourseStatistics(Course course)
        {
            var totalEnrollments = course.GetTotalEnrollments();
            var totalPayments = course.GetTotalPayments(allStudents);
            Console.WriteLine($"\nCourse Statistics for {course.CourseName}:");
            Console.WriteLine($"Total Enrollments: {totalEnrollments}");
            Console.WriteLine($"Total Payments: {totalPayments:C}");
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Student student1 = new Student { StudentID = 1, FirstName = "John", LastName = "Doe", DateOfBirth = new DateTime(2000, 5, 15), Email = "john.doe@example.com", PhoneNumber = "123-456-7890" };
            Course course1 = new Course { CourseID = 101, CourseName = "Intro to C#", CourseCode = "CS101", InstructorName = "Dr. Smith" };
            Teacher teacher1 = new Teacher { TeacherID = 201, FirstName = "Sarah", LastName = "Connor", Email = "sarah.connor@example.com" };

            SIS sis = new SIS();

            sis.EnrollStudentInCourse(student1, course1);
            sis.AssignTeacherToCourse(teacher1, course1);
            sis.RecordPayment(student1, 500.00m, DateTime.Now);

            student1.DisplayStudentInfo();
            course1.DisplayCourseInfo();
            teacher1.DisplayTeacherInfo();

            sis.GenerateEnrollmentReport(course1);
            sis.GeneratePaymentReport(student1);
            sis.CalculateCourseStatistics(course1);

            Console.ReadLine();
        }
    }
}