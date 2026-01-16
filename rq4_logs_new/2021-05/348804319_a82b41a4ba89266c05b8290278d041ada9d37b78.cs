using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TestDiplom.Models;
using TestDiplom.Models.StudentSubject;

namespace TestDiplom.Controllers.StudentSubject
{
    [Route("api/[controller]")]
    [ApiController]
    public class StudentSubjectController : ControllerBase
    {
        private readonly AuthenticationContext _context;
        public StudentSubjectController(AuthenticationContext context)
        { 
            _context = context;
        }

        [HttpPost]
        [Authorize]
        [Route("CreateOrEdit")]
        //POST : /api/StudentSubject/CreateOrEdit

        public async Task CreateOrEdit(StudentSubjectModel model)
        {
            if (model.Id == 0)
            {
                await Create(model);
            }
            else
            {
                await Update(model);
            }
        }

        [HttpPost]
        [Authorize(Roles = "Student")]
        [Route("Create")]
        //POST : /api/StudentSubject/Create
        public async Task<Object> Create(StudentSubjectModel model)
        {
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var studentSubject = new Models.StudentSubject.StudentSubject()
            {
                IsSubscribe = model.IsSubscribe,
                StudentId = userId,
                SubjectId = model.SubjectId
            };
            try
            {
                await _context.StudentSubjects.AddAsync(studentSubject);
                await _context.SaveChangesAsync();

                return Ok();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [HttpPost]
        [Authorize(Roles = "Student")]
        [Route("Update")]
        //POST : /api/StudentSubject/Update

        public async Task Update(StudentSubjectModel model)
        {

            var updateStudentSubject = await _context.StudentSubjects.FirstOrDefaultAsync(s => s.Id == model.Id);

            updateStudentSubject.IsSubscribe = model.IsSubscribe;
            updateStudentSubject.StudentId = model.StudentId;
            updateStudentSubject.SubjectId = model.SubjectId;

            _context.StudentSubjects.Update(updateStudentSubject);
            await _context.SaveChangesAsync();
        }

        [HttpGet]
        [Authorize(Roles = "Student")]
        [Route("GetById")]
        //GET : /api/StudentSubject/GetById

        public async Task<StudentSubjectModel> GetById(int id)
        {
            var s = await _context.StudentSubjects.Include(s => s.SubjectFk).Where(s => s.Id == id).FirstOrDefaultAsync();

            var ret = new StudentSubjectModel
            {
                Id = s.Id,
                SubjectId = s.SubjectId,
                StudentId = s.StudentId,
                IsSubscribe = s.IsSubscribe,
                SubjectName = s.SubjectFk.Name
            };
            return ret;
        }

        [HttpGet]
        [Authorize]
        [Route("GetAll")]
        //GET : /api/StudentSubject/GetAll
        public async Task<List<StudentSubjectModel>> GetAll()
        {
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var sub = await _context.StudentSubjects
                .Include(s => s.SubjectFk)
                .Where(s => s.StudentId == userId && s.IsSubscribe == true).ToListAsync();

            var studentSubjects = new List<StudentSubjectModel>();

            foreach (var item in sub)
            {
                var s = new StudentSubjectModel
                {
                    Id = item.Id,
                    IsSubscribe = item.IsSubscribe,
                    StudentId = item.StudentId,
                    SubjectId = item.SubjectId,
                    SubjectName = item.SubjectFk.Name,

                };

                studentSubjects.Add(s);
            }

            return studentSubjects;
        }


    }
}