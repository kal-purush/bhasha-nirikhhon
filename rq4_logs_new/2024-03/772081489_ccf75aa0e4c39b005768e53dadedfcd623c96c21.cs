using Examination.Models;
using Examination.Repo;
using Microsoft.AspNetCore.Mvc;

namespace Examination.Controllers
{
    public class AStudentController : Controller
    {

        private readonly AStudentRepo _stdrepo;

        public AStudentController(AStudentRepo stdrepo)
        {
            _stdrepo = stdrepo;
        }
        public IActionResult Display()
        {
            var model = _stdrepo.GetAllAtudents();
            int studentCount = _stdrepo.GetStudentCount();
            ViewBag.StudentCount = studentCount;
            return View(model);
        }
        public IActionResult Delete(int? id)
        {
            if (id == null)
            {
                return BadRequest();
            }

            _stdrepo.Delete(id.Value);

            return RedirectToAction("Display");
        }
        public IActionResult GetStudentDetails(int? id)
        {
            if (id == null)
            {
                return BadRequest();
            }

            var student = _stdrepo.GetStudentDetailsById(id.Value);

            if (student == null)
            {
                return NotFound();
            }

            return View(student);
        }
        public IActionResult Edit(int? id)
        {
            if (id == null)
            {
                return BadRequest();
            }

            var studentedit = _stdrepo.GetStudentDetailsById(id.Value);

            if (studentedit == null)
            {
                return NotFound();
            }

            ViewBag.Courses = _stdrepo.GetCourses();
            ViewBag.Tracks = _stdrepo.GetTracks();

            return View(studentedit);
        }

        [HttpPost]
        public IActionResult Edit(Student student)
        {
            try
            {
                Console.WriteLine($"Received student data: {student.Sid}, {student.Sname}, {student.Semail}, {student.Password}, {student.Sgender}, {student.TrackId}, {student.CrsId}");

                _stdrepo.UpdateStudent(student.Sid, student.Sname, student.Semail, student.Password, student.Sgender, student.TrackId, student.CrsId);

                Console.WriteLine("Student details updated successfully.");
                ViewBag.ConfirmationMessage = "Student details updated successfully.";
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating student: {ex.Message}");
                ViewBag.ErrorMessage = "An error occurred while updating the student.";
            }

            return RedirectToAction("Display");
        }



        public IActionResult Add()
        {
           
            ViewBag.Courses = _stdrepo.GetCourses();
            ViewBag.Tracks = _stdrepo.GetTracks();
            return View(new Student());
        }

        [HttpPost]
        public IActionResult Add(Student student)
        {
            try
            {
                _stdrepo.AddStudent(student.Sname, student.Semail, student.Password, student.Sgender, student.TrackId);
                ViewBag.ConfirmationMessage = "Student details added successfully.";
            }
            catch (Exception ex)
            {
                ViewBag.ErrorMessage = "An error occurred while adding the student.";
                // Log the error if necessary
            }

            return RedirectToAction("Display");
        }



    }
}