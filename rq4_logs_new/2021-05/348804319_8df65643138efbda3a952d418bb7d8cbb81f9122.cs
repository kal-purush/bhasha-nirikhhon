using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TestDiplom.Models;
using TestDiplom.Models.Practice;

namespace TestDiplom.Controllers.practice
{
    [Route("api/[controller]")]
    [ApiController]
    public class PracticeController : ControllerBase
    {
        private readonly AuthenticationContext _context;
        public PracticeController(AuthenticationContext context)
        {
            _context = context;
        }

        [HttpPost]
        [Authorize]
        [Route("CreateOrEdit")]
        //POST : /api/Practice/CreateOrEdit

        public async Task CreateOrEdit(PracticeModel model)
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

        [HttpGet]
        [Authorize]
        [Route("GetAllPracticeForUser")]
        //GET : /api/Practice/GetAllPracticeForUser
        public List<PracticeModel> GetAllPracticeForUser()
        {
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var list = _context.Practices.Where(p => p.OwnerId == userId);

            var lst = new List<PracticeModel>();

            foreach (var item in list)
            {
                var practice = new PracticeModel();

                practice.Id = item.Id;
                practice.Name = item.Name;
                practice.OwnerId = item.OwnerId;
                practice.PracticeFiles = GetAllPracticeFilesById(item.Id);

                lst.Add(practice);
            }

            return lst;
        }

        [HttpGet]
        [Authorize]
        [Route("GetPracticeById")]
        //GET : /api/Practice/GetPracticeById
        public async Task<PracticeModel> GetPracticeById(string Id)
        {
            var id = Convert.ToInt32(Id);
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var prac = await _context.Practices.Where(p => p.Id == id).FirstOrDefaultAsync();

            var practice = new PracticeModel
            {
                Id = prac.Id,
                Name = prac.Name,
                OwnerId = prac.OwnerId,
                PracticeFiles = GetAllPracticeFilesById(prac.Id)
            };

            return practice;
        }

        [HttpGet]
        [Authorize]
        [Route("GetAllPracticeFilesById")]
        //GET : /api/Practice/GetAllPracticeFilesById
        public List<PracticeFiles> GetAllPracticeFilesById(int id)
        {
            var files = _context.PracticeFiles.Where(f => f.PracticeId == id);

            var filesArr = new List<PracticeFiles>();

            foreach (var item in files)
            {
                var f = new PracticeFiles();
                f.Path = item.Path;
                f.FileName = item.FileName;
                filesArr.Add(f);
            }

            return filesArr;

        }

        [HttpPost]
        [Authorize]
        [Route("Create")]
        //POST : /api/Practice/Create
        public async Task<Object> Create(PracticeModel model)
        {
            string userId = User.Claims.First(c => c.Type == "UserID").Value;

            var practice = new Practice()
            {
                Name = model.Name,
                OwnerId = userId,
            };
            try
            {
                await _context.Practices.AddAsync(practice);
                await _context.SaveChangesAsync();

                var Id = practice.Id;

                await InsertFiles(model.PracticeFiles, Id);

                return Ok();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        protected async Task InsertFiles(IList<PracticeFiles> files, int id)
        {
            for (int i = 0; i < files.Count; i++)
            {
                var file = new PracticeFiles
                {
                    FileName = files[i].FileName,
                    Path = files[i].Path,
                    PracticeId = id,
                };

                await _context.PracticeFiles.AddAsync(file);
                await _context.SaveChangesAsync();
            }
        }

        [HttpPost]
        [Authorize]
        [Route("UpdatePractice")]
        //POST : /api/Practice/UpdatePractice

        public async Task Update(PracticeModel model)
        {

            var updatePractice = await _context.Practices.FirstOrDefaultAsync(p => p.Id == model.Id);

            updatePractice.Name = model.Name;

            await DeletePracticeFiles(model.Id);

            await InsertFiles(model.PracticeFiles, model.Id);

            await _context.SaveChangesAsync();
        }

        protected async Task<List<PracticeFiles>> DeletePracticeFiles(int id)
        {
            var deleteItem = await _context.PracticeFiles.Where(p => p.PracticeId == id).ToListAsync();
            if (deleteItem != null)
            {
                _context.PracticeFiles.RemoveRange(deleteItem);
                _context.SaveChanges();
            }
            return deleteItem;
        }

        [HttpDelete]
        [Authorize]
        [Route("Delete")]
        //GET : /api/Practice/Delete
        public Practice Delete(int id)
        {
            var deleteItem = _context.Practices.FirstOrDefault(p => p.Id == id);
            if (deleteItem != null)
            {
                _context.Practices.Remove(deleteItem);
                _context.SaveChanges();
            }
            return deleteItem;
        }
    }
}