using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using easyGrading.Models;
using easyGrading.Services.Interface;
using Microsoft.AspNetCore.Mvc;

namespace easyGrading.Controllers
{
    public class TaController : Controller
    {
        static int ID;
        static string CourseId;
        static string FirstName;
        static string LastName;
        static int StudentId;
        private IDbQueries _dbQueries;
        static int GradeId;
        static string Course_Section_Name;
        public TaController(IDbQueries dbQueries)
        {
            _dbQueries = dbQueries;
        }
        public IActionResult TaPage(int id)
        {
            ID = id;
            return RedirectToAction("Home", "Ta");
        }

        public IActionResult Home()
        {
            Ta ta = _dbQueries.GetTa(ID);
            CourseId = ta.Course_Id;
            ViewBag.courseId = ta.Course_Id;
            var model = _dbQueries.ReturnAllStudent(ta.Course_Id);
            return View(model);
            
        }

        public string GetSectionName()
        {
            return "assignment1";
        }

        public IActionResult ShowGrade(int id)
        {
            Student st = _dbQueries.GetStudent(id);
            FirstName = st.FirstName;
            LastName = st.LastName;
            StudentId = st.Id;

            ViewBag.Name = FirstName+"."+LastName;
            ViewBag.StudentId = StudentId;
            ViewBag.courseId = CourseId;

            
            var classes = new List<ShowGradeViewModel>();

                var model = _dbQueries.ReturnAllGrade(StudentId, CourseId);

                foreach (var grade in model)
                {
                    var gradeClass = new ShowGradeViewModel();
                    gradeClass.Id = grade.Id;
                    gradeClass.Student_Id = grade.Student_Id;
                    gradeClass.Course_Outline_Id = grade.Course_Outline_Id;
                    gradeClass.Course_Id = grade.Course_Id;
                    gradeClass.Actual_Grade = grade.Actual_Grade;
                    gradeClass.Editable = grade.Editable;
                    var temp = _dbQueries.GetCourseOutline(grade.Course_Outline_Id);
                    gradeClass.Course_Outline_Part = temp.Part;
                    gradeClass.Percentage = temp.Percentage;
                    classes.Add(gradeClass);

                }


            return View(classes);

        }
        [HttpGet]
        public IActionResult EditGrade(int id, string name)
        {
            GradeId = id;
            Course_Section_Name = name;
            Grade model = _dbQueries.GetGrade2(id);
            ViewBag.Name = Course_Section_Name;
            ViewBag.courseId = CourseId;
            ViewBag.StudentName = FirstName + "." + LastName;
            return View(model);
        }
        [HttpPost]
        public IActionResult EditGrade(Grade model, string button)
        {
            if (button == "cancel")
            {
                return RedirectToAction("ShowGrade", "Ta", new {id= StudentId });
            }
            if (model.Actual_Grade < 110 && model.Actual_Grade>0) 
            {
                _dbQueries.UpdateGrade(GradeId, model.Actual_Grade, model.Editable);
                return RedirectToAction("ShowGrade", "Ta", new { id = StudentId });
            }
            else
            {
                TempData["Message"] = "Please enter a resonable grade";
            }
            ViewBag.Name = Course_Section_Name;
            ViewBag.courseId = CourseId;
            ViewBag.StudentName = FirstName + "." + LastName;
            return View(model);
        }


        #region EditAccount
        [HttpGet]
        public IActionResult EditAccount()
        {
            Ta model = _dbQueries.GetTa(ID);
            return View(model);
        }
        [HttpPost]
        public IActionResult EditAccount(Ta model)
        {
            if (model.Password != null && model.Name != null)
            {
                model.Id = ID;
                _dbQueries.TaUpdate(model);
                TempData["SuccessMessage"] = "Account has been updated Successfully";
            }
            if (model.Name == null) { TempData["message2"] = "Name cannot be null"; }
            if (model.Password==null) { TempData["message"] = "Password cannot be null"; }
            return View(model);
        }

        #endregion
    }
}