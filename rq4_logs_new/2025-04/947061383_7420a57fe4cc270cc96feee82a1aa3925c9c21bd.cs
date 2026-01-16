using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;
using System.Collections;
using StudentInformationSystem;

namespace StudentInformationSystem
{
    //TASK 2 IMPLEMENTATION OF SIS CLASS
    public class SIS
    {
        private List<Student> students = new();
        public void EnrollStudentInCourse(Student student, Course course)
        {
            // Implementation for enrolling a student in a course
            student.EnrollInCourse(course);
        }

        public void AssignTecherToCourse(Teacher teacher, Course course)
        {
            // Implementation for assigning a teacher to a course
            course.AssignTeacher(teacher);
        }


        public void RecordPayments(Student student, decimal amount, DateTime PaymentDate)
        {
            // Implementation for recording payments
            Payment payment = new Payment
            {
                StudentID= student,
                Amount= amount,
                paymentDate = PaymentDate
            };
            student.MakePayment(amount,PaymentDate);

        }

        public void GenerateEnrollmentReport(Student student, Enrollment enrollment, Course course)
        {
            // Implementation for generating enrollment report
            console.writeLine($"enrollment report for{course.CourseName}:");
            foreach (var enr in course.GetEnrollments())
            {
                Console.WriteLine($"Enrollment ID: {enr.EnrollmentID},Student ID:{student.StudentID},Date:{enr.EnrollmentDate}");

            }

        }

        public void GeneratePaymentReport(Student student, Payment payment)
        {
            // Implementation for generating payment report
            Console.WriteLine($"Payment Report for {student.FirstName} {student.LastName}:");
            foreach (var pay in student.GetPaymentHistory())
            {
                Console.WriteLine($"Payment ID: {pay.PaymentID}, Amount: {pay.Amount}, Date: {pay.PaymentDate}");
            }
            //Console.WriteLine($"Payment ID: {payment.PaymentID}, Amount: {payment.Amount}, Date: {payment.PaymentDate}");
        }

        public void GenerateCourseStatics(Course course)
        {
            // Implementation for generating course statistics
            Console.WriteLine($"Course Statistics for {course.CourseName}:");
            Console.WriteLine($"Total Enrollments: {course.GetTotalEnrollments()}");
            Console.WriteLine($"Total Payments: {course.GetTotalPayments()}");

        }

    }
	
}