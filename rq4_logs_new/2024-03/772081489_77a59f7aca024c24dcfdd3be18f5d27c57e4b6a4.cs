using Examination.Models;
using Examination.Repo;
using Examination.ViewModel;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;

namespace Examination.Controllers
{
    public class QuestionsController : Controller
    {
        private readonly IquestionRepo _questionRepo;
        private readonly AStudentRepo _studentRepo;

        public QuestionsController(IquestionRepo questionRepo, AStudentRepo studentRepo)
        {
            _questionRepo = questionRepo;
            _studentRepo = studentRepo;
        }

        [HttpGet]
        public IActionResult Add()
        {
            ViewBag.Course = _studentRepo.GetCourses();
            return View();
        }

        [HttpPost]
        public IActionResult Add(QuestionAnswersModel model)
        {
            if (ModelState.IsValid)
            {
                var question = new Question
                {
                    Qbody = model.Qbody,
                    Qtype = model.Qtype,
                    Qmark = model.Qmark,
                    CrId = model.CrId
                };

                int qid = _questionRepo.AddQuestion(question);
                if (qid == 0)
                {
                    TempData["Error"] = "There was an error please try again";
                }
                else
                {
                    TempData["Message"] = "Added Successfully";
                }
                foreach (var answer in model.Answers)
                {
                    var answerr = new Answer
                    {
                        Answerbody = answer.Answerbody,
                        AnswerId = answer.AnswerId,
                        IsCorrect = answer.IsCorrect,
                        QId = qid
                    };

                    int aid = _questionRepo.AddAnswers(answerr);
                    if (aid == 0)
                    {
                        TempData["Error"] = "There was an error please try again";
                    }
                    else
                    {
                        TempData["Message"] = "Added Successfully";
                    }
                }

                return RedirectToAction("Add");
            }

            ViewBag.Course = _studentRepo.GetCourses();
            return View(model);
        }

    }
}