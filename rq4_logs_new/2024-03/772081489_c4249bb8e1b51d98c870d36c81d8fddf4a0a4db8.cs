using Examination.Models;
using Examination.Repo;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing.Constraints;

namespace Examination.Controllers
{
    public class StudentController : Controller
    {
        IStudentRepo Srepo;
        public StudentController(IStudentRepo _StudRepo)
        {
            Srepo=_StudRepo;

        }

        [HttpGet]
        public IActionResult StartExam(int ExamId)
        {
           List<Question> questions= Srepo.GetExamQuestions(1);
           ViewBag.questions = questions;
           ViewBag.ExamName =Srepo.GetCourseById(Srepo.GetExam(1).CrId).Cname;
           ViewBag.NumberOfQuestion = questions.Count();
           TimeOnly endTime = Srepo.GetExam(1).EndTime;
           TimeOnly startTime = Srepo.GetExam(1).StartTime;
           TimeSpan examDuration = endTime - startTime;
           ViewBag.ExamDuration = examDuration.TotalMinutes;
           return View();
        }
        [HttpPost]
        public IActionResult StartExam(Dictionary<int, Student_Answer> answers)
        {
            List<Question> questions = Srepo.GetExamQuestions(1);
            ViewBag.questions = questions;
            ViewBag.ExamName = Srepo.GetCourseById(Srepo.GetExam(1).CrId).Cname;
            ViewBag.NumberOfQuestion = questions.Count();
            TimeOnly endTime = Srepo.GetExam(1).EndTime;
            TimeOnly startTime = Srepo.GetExam(1).StartTime;
            TimeSpan examDuration = endTime - startTime;
            ViewBag.ExamDuration = examDuration.TotalMinutes;
            Student_Answer std = new Student_Answer();
            foreach (var entry in answers)
            {
                std.QId = entry.Key;
                Student_Answer answer = entry.Value;
                std.SId = answer.SId;
                std.EId = answer.EId;
                std.SAnswer = answer.SAnswer ?? "0";
                Srepo.StudentAnswers(std);
            }
            int Eid = answers.Values.First().EId;
            int Sid = answers.Values.First().SId;

            return RedirectToAction("Result", new { Eid = Eid, Sid = Sid });
        }
        public IActionResult Result(int Eid,int Sid)
        {
            ViewBag.Score = Srepo.CalculateExamScore(Eid, Sid);
            ViewBag.TotalScore = Srepo.CalculateTotalExamScore(Eid);
            return View("Result");
        }
    }
}