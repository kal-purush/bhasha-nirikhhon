using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TestDiplom.Models;
using TestDiplom.Models.Statistic;

namespace TestDiplom.Controllers.Statistic
{
    [Route("api/[controller]")]
    [ApiController]
    public class StatisticController : ControllerBase
    {
        private readonly AuthenticationContext _context;
        private readonly UserManager<ApplicationUser> _userManager;

        public StatisticController(AuthenticationContext context, UserManager<ApplicationUser> userManager)
        {
            _context = context;
            _userManager = userManager;
        }

        [HttpGet]
        [Authorize(Roles = "Student")]
        [Route("GetForStudent")]
        //GET : /api/Statistic/GetForStudent
        public async Task<List<ChartModel>> GetForStudent()
        {
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var practiceCount = 0;
            var testCount = 0;
            double avgPracticeScore = 0;
            double avgTestScore = 0;

            var lst = new List<ChartModel>();

            var subjects = await _context.StudentSubjects.Where(s => s.StudentId == userId && s.IsSubscribe == true).ToListAsync();

            foreach (var item in subjects)
            {
                practiceCount += _context.Practices.Where(p => p.SubjectId == item.SubjectId).Select(q => q).Count();
            }

            var studentPractice = await _context.StudentPractices.Where(s => s.StudentId == userId).ToListAsync();

            foreach (var item in studentPractice)
            {
                avgPracticeScore += item.PracticeScore;
            }

            var allDoneStudentPractice = CreateChartModel($"Всего сделано практических заданий из {practiceCount}", studentPractice.Count);

            lst.Add(allDoneStudentPractice);

            var avgStudentPracticeScore = CreateChartModel("Средний балл по практическим заданиям", avgPracticeScore / practiceCount);

            lst.Add(avgStudentPracticeScore);

            foreach (var item in subjects)
            {
                testCount += _context.Tests.Where(p => p.SubjectId == item.SubjectId).Select(q => q).Count();
            }

            var studentTest = await _context.StudentTestings.Where(s => s.StudentId == userId).ToListAsync();

            foreach (var item in studentTest)
            {
                avgTestScore += item.TestScore;
            }

            var allDoneStudentTesting = CreateChartModel($"Всего пройдено тестов из {testCount}", studentTest.Count);

            lst.Add(allDoneStudentTesting);

            var avgStudentTestingScore = CreateChartModel("Средний балл по тестированию", avgTestScore / testCount);

            lst.Add(avgStudentTestingScore);

            return lst;
        }

        protected ChartModel CreateChartModel(string name, double value)
        {
            ChartModel chartModel = new ChartModel
            {
                Name = name,
                Value = Math.Round(value, 2)
            };

            return chartModel;
        }

        [HttpGet]
        [Authorize(Roles = "Teacher")]
        [Route("GetForTeacher")]
        //GET : /api/Statistic/GetForTeacher
        public async Task<List<ChartModel>> GetForTeacher()
        {
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var lst = new List<ChartModel>();

            var practice = await _context.StudentPractices
                .Include(s => s.PracticeFk)
                .Where(s => s.PracticeFk.OwnerId == userId).ToListAsync();

            var isAcceptAmount = practice.Where(p => p.IsAccept == true).Select(p=> p).Count();
            var isRevisionAmount = practice.Where(p => p.IsRevision == true).Select(p => p).Count();
            var isCheckAmount = practice.Where(p => p.IsRevision == false && p.IsAccept == false).Select(p => p).Count();

            var a = CreateChartModel($"Всего принятых практических заданий из {practice.Count}", isAcceptAmount);
            lst.Add(a);

            var r = CreateChartModel($"Всего отправленных на доработку практических заданий из {practice.Count}", isRevisionAmount);
            lst.Add(r);

            var c = CreateChartModel($"Всего практических заданий на проверке из {practice.Count}", isCheckAmount);
            lst.Add(c);

            return lst;
        }

        [HttpGet]
        [Authorize(Roles = "Admin")]
        [Route("GetForAdmin")]
        //GET : /api/Statistic/GetForAdmin
        public async Task<List<ChartModel>> GetForAdmin()
        {
            var lst = new List<ChartModel>();

            var users = await _userManager.Users.Where(u => u.UserName != "Admin").ToListAsync();

            var blockedUser = users.Where(u => u.IsBlocked == true).Count();
            var b = CreateChartModel($"Всего заблокированных пользователей из {users.Count}", blockedUser);
            lst.Add(b);

            var activeUser = users.Where(u => u.IsBlocked == false || u.IsBlocked == null).Count();
            var a = CreateChartModel($"Всего активных пользователей из {users.Count}", activeUser);
            lst.Add(a);

            var teacherAmount = 0;
            var studentAmount = 0;

            foreach (var u in users)
            {
                var role = await _userManager.GetRolesAsync(u);

                if (role.FirstOrDefault() == "Teacher")
                {
                    teacherAmount += 1;
                }
                else
                {
                    studentAmount += 1;
                }
            }

            var t = CreateChartModel($"Всего преподователей из {users.Count}", teacherAmount);
            lst.Add(t);

            var s = CreateChartModel($"Всего студентов из {users.Count}", studentAmount);
            lst.Add(s);

            return lst;
        }
    }
}