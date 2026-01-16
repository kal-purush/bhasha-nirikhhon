using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using easyGrading.Models;
using easyGrading.Services.Interface;
using easyGrading.Services.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace easyGrading.Controllers
{
    public class AdminController : Controller
    {
        static int ID;
        static int workDep_Id;
        static int ProfId;
        static string ProfName;
        private IDbQueries _dbQueries;
        static Course currentModel;

        public AdminController(IDbQueries dbQueries)
        {
            _dbQueries = dbQueries;
        }
        public IActionResult AdminPage(int id)
        {
            ID = id;
            return RedirectToAction("Home", "Admin", new { d="DepartmentCourse" });
        }

        public void SearchDepartment()
        {
            string direction = "EditDepartment";
            Home(direction);
        }
        public void SearchCourse()
        {
            string direction = "DepartmentCourse";
            Home(direction);
        }
        public IActionResult Home(string d)
        {
            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.direction = d;
            return View(list);
        }

        public IActionResult DepartmentCourse(int id)
        {
            IEnumerable<Course> list = _dbQueries.returnAllCourse(id);
            Department result = _dbQueries.returnDepartName(id);

            ViewBag.departmentName = result.Name;
            return View(list);
        }


        #region AddCourse
        [HttpGet]
        public IActionResult AddCourse()
        {
            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name");

            IEnumerable<Professor> list2 = _dbQueries.returnAllProfessor();
            ViewBag.ProfessorList = new SelectList(list2, "Id", "Name");

            return View();
        }

        [HttpPost]
        public IActionResult AddCourse(Course model)
        {
            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name");

            IEnumerable<Professor> list2 = _dbQueries.returnAllProfessor();
            ViewBag.ProfessorList = new SelectList(list2, "Id", "Name");

            if (model.Id != null && model.Name != null)
            {
                var result2 = _dbQueries.checkCourseName(model.Name);
                var result = _dbQueries.CheckCourseId(model.Id);

                if (result)
                {
                    TempData["Message"] = "This course Id is alredy exist";
                }
                if (result2)
                {
                    TempData["Message2"] = "This course name is alredy exist";
                }
                if (!result && !result2)
                {                 
                    Course course = new Course();
                    course.Id = model.Id;
                    course.Name = model.Name;
                    course.Dep_Id = model.Dep_Id;
                    course.Prof_Id = model.Prof_Id;
                    course.Prof_Id2 = model.Prof_Id;

                    _dbQueries.SaveCourse(course);

                    TempData["SuccessMessage"] = "Course Added Successfully";
                }
            }
            else
            {
                if (model.Id == null)
                {
                    TempData["Message"] = "Course Id field cannot be empty";
                }
                if (model.Name == null)
                {
                    TempData["Message2"] = "Course Name field cannot be empty";
                }
            }

            return View();
        }
        #endregion


        #region EditAccount
        [HttpGet]
        public IActionResult EditAccount()
        {
            Admin model = _dbQueries.GetAdmin(ID);
            ViewBag.adminInfo = model;
            return View(model);
        }
        [HttpPost]
        public IActionResult EditAccount(Admin model)
        {
            _dbQueries.AdminUpdate(model);
            TempData["SuccessMessage"] = "Account has been updated Successfully";
            return View(model);
        }

        #endregion


        #region EditDepartment

        [HttpGet]
        public IActionResult EditDepartment(int id)
        {
            Department model = _dbQueries.returnDepartName(id);
            ViewBag.adminInfo = model;
            return View(model);
        }
        [HttpPost]
        public IActionResult EditDepartment(Department model, string button)
        {
            if (button == "update")
            {
                if (model.Name != null)
                {
                    _dbQueries.DepartmentUpdate(model);
                    TempData["SuccessMessage"] = "Department has been updated Successfully";
                }
                else
                {
                    TempData["Message2"] = "Department Name cannot be empty";
                }
            }
            else
            {
                _dbQueries.ReSignCourseDepartment(model.Id);
                _dbQueries.ReSignStudentMajor(model.Id);
                _dbQueries.WorkInDelete(model.Id, "department");
                _dbQueries.DepartmentDelete(model);
                TempData["SuccessMessage"] = "Department has been deleted Successfully";
            }
            return View(model);
        }

        #endregion


        #region EditCourse

        [HttpGet]
        public IActionResult EditCourse(string id)
        {
            Course model = _dbQueries.GetCourse(id);
            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name", model.Dep_Id);

            IEnumerable<Professor> list2 = _dbQueries.returnAllProfessor();
            ViewBag.ProfessorList = new SelectList(list2, "Id", "Name", model.Prof_Id);

            ViewBag.ProfessorList2 = new SelectList(list2, "Id", "Name", model.Prof_Id2);
            currentModel = model;
            return View(model);
        }
        [HttpPost]
        public IActionResult EditCourse(Course model, string button)
        {
            if (button == "update")
            {
                if (model.Name != null)
                {
                    bool result2 = false;
                    if(currentModel.Name != model.Name) { result2 = _dbQueries.checkCourseName(model.Name); }
                                       
                    if (result2)
                    {
                        TempData["Message2"] = "This course name is alredy exist";
                    }
                    if (!result2)
                    {
                        _dbQueries.CourseUpdate(model);

                        currentModel = model;

                        TempData["SuccessMessage"] = "Course Updated Successfully";
                        return RedirectToAction("DepartmentCourse", "Admin", new { id = model.Dep_Id });
                    }
                }
                else
                {
                    if (model.Name == null)
                    {
                        TempData["Message2"] = "Course Name field cannot be empty";
                    }
                }
            }
            else
            {
               // _dbQueries.GradeDelete(model.Id);
               // _dbQueries.TakesDelete(model.Id);
                _dbQueries.CourseOutlineDelete(model.Id);

                _dbQueries.ReSignTa(model.Id);
                _dbQueries.CourseDelete(model);
                currentModel = model;
                TempData["SuccessMessage"] = "course has been deleted Successfully";
            }

            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name", model.Dep_Id);

            IEnumerable<Professor> list2 = _dbQueries.returnAllProfessor();
            ViewBag.ProfessorList = new SelectList(list2, "Id", "Name", model.Prof_Id);

            ViewBag.ProfessorList2 = new SelectList(list2, "Id", "Name", model.Prof_Id2);
            return View(model);
        }

        #endregion


        #region AddDepartment
        [HttpGet]
        public IActionResult AddDepartment()
        {
            return View();
        }

        [HttpPost]
        public IActionResult AddDepartment(Department model)
        {
            if (model.Id >0 && model.Name != null)
            {
                var result2 = _dbQueries.checkDepartmentName(model.Name);
                var result = _dbQueries.CheckDepartmentId(model.Id);

                if (result)
                {
                    TempData["Message"] = "This department Id is alredy exist";
                }
                if (result2)
                {
                    TempData["Message2"] = "This department name is alredy exist";
                }
                if (!result && !result2)
                {
                    Department department = new Department();
                    department.Id = model.Id;
                    department.Name = model.Name;
                    department.Admin_Id = ID;

                    _dbQueries.SaveDepartment(department);

                    TempData["SuccessMessage"] = "Department Added Successfully";
                }
            }
            else
            {
                if (model.Id <=0)
                {
                    TempData["Message"] = "Department Id field cannot be empty";
                }
                if (model.Name == null)
                {
                    TempData["Message2"] = "Department Name field cannot be empty";
                }
            }

            return View();
        }
        #endregion


        #region AddProfessor
        [HttpGet]
        public IActionResult AddProfessor()
        {
            return View();
        }

        [HttpPost]
        public IActionResult AddProfessor(Professor model)
        {

            if (model.Id >0 && model.Name != null)
            {
                var result = _dbQueries.isProf(model.Id);
                var result2= _dbQueries.isStudent(model.Id);
                var result3 = _dbQueries.isAdmin(model.Id);

                if (result || result2 || result3)
                {
                    TempData["Message"] = "This Professor Id is alredy exist";
                }
                if (!result)
                {
                    Professor professor = new Professor();
                    professor.Id = model.Id;
                    professor.Name = model.Name;
                    professor.Password = model.Password;
                    professor.Admin_Id = ID;

                    _dbQueries.SaveProfessor(professor);

                    Work_in work = new Work_in();
                    work.Prof_Id = model.Id;
                    work.Dep_Id = 0;
                    _dbQueries.SaveWorkin(work);

                    TempData["SuccessMessage"] = "Professor Added Successfully";
                }
            }
            else
            {
                if (model.Id <=0)
                {
                    TempData["Message"] = "Id field cannot be empty";
                }
                if (model.Name == null)
                {
                    TempData["Message2"] = "Name field cannot be empty";
                }
            }

            return View();
        }
        #endregion


        #region EditProfessor
        public IActionResult SearchProfessor()
        {
            IEnumerable<Professor> list = _dbQueries.returnAllProfessor2();
            return View(list);
        }
        [HttpGet]
        public IActionResult EditProfessor(int id)
        {
            Professor model = _dbQueries.GetProf(id);
            IEnumerable<Work_in> list = _dbQueries.returnAllWorkIn(id);
            ViewBag.ProfName = model.Name;
            ViewBag.ProfId = model.Id;

            ProfId= model.Id;
            ProfName= model.Name;

            IEnumerable<Department> list2 = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list2, "Id", "Name");

            return View(list);
        }

        [HttpGet]
        public IActionResult EditWorkIn(int id)
        {
            Work_in work = _dbQueries.GetWorkIn(ProfId ,id);
            workDep_Id = work.Dep_Id;            
            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name", work.Dep_Id);
            ViewBag.ProfName =ProfName;
            return View();
        }

        [HttpPost]
        public IActionResult EditWorkIn(Work_in model, string button)
        {
            if (button == "delete") 
            {
                _dbQueries.WorkInDelete2(ProfId, workDep_Id);
                return RedirectToAction("EditProfessor", "Admin", new { id = ProfId });
            }
            var result = _dbQueries.CheckWorkIn(ProfId, model.Dep_Id);
            if (!result)
            {
                _dbQueries.WorkInUpdate(ProfId, workDep_Id, model.Dep_Id);
                return RedirectToAction("EditProfessor", "Admin", new { id = ProfId });
            }

            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name", model.Dep_Id);
            TempData["Message"] = "Already worked in this department";
            return View();

        }

        [HttpPost]

        public IActionResult EditProfessor(string button)
        {
            if (button == "delete") {            
                _dbQueries.ReSignCourseProf(ProfId);
                _dbQueries.ReSignTaProf_Id(ProfId);
                _dbQueries.WorkInDelete(ProfId, "prof");
                _dbQueries.ProfessorDelete(_dbQueries.GetProf(ProfId));
                return RedirectToAction("SearchProfessor", "Admin");
            }
            
            return RedirectToAction("AddWorkIn", "Admin");
        }
        [HttpGet]
        public IActionResult AddWorkIn()
        {

            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name");
            ViewBag.profName = ProfName;
            return View();
        }

        [HttpPost]
        public IActionResult AddWorkIn(Work_in model)
        {
            if (model.Dep_Id >= 0) 
            {
                var result = _dbQueries.CheckWorkIn(ProfId, model.Dep_Id);
                if (!result) {
                    Work_in work = new Work_in();
                    work.Prof_Id = ProfId;
                    work.Dep_Id = model.Dep_Id;
                    _dbQueries.SaveWorkin(work);
                    return RedirectToAction("EditProfessor", "Admin", new {id= ProfId }); 
                }
                TempData["Message"] = "Already worked in this department";
            }

            IEnumerable<Department> list = _dbQueries.returnAllDepartment();
            ViewBag.DepartmentList = new SelectList(list, "Id", "Name");
            return View();
        }
        #endregion
    }
}