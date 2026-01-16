using CoreProject_DataAccess.Data;
using CoreProject_Models.Models;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CoreProject.Controllers
{
    public class AuthorController : Controller
    {
        private readonly ApplicationDbContext _dbModel;
        public AuthorController(ApplicationDbContext dbModel)
        {
            _dbModel = dbModel;
        }
        public ActionResult Index()
        {
            var authors = _dbModel.Authors.ToList();
            return View(authors);
        }

        public ActionResult Upsert(int? id)
        {
            var authors = new Author();
            if (id != null)
            {
                authors = _dbModel.Authors.FirstOrDefault(x => x.Author_Id.Equals(id));
                if (authors == null)
                {
                    return NotFound();
                }
            }
            return View(authors);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Upsert(Author authors)
        {
            if (ModelState.IsValid)
            {
                if (authors.Author_Id == 0)
                {
                    //create
                    _dbModel.Add(authors);
                }
                else
                {
                    //update
                    _dbModel.Update(authors);
                }
                _dbModel.SaveChanges();
                return RedirectToAction("Index");
            }
            return View(authors);
        }

        public ActionResult Delete(int id)
        {
            var author = _dbModel.Authors.FirstOrDefault(x => x.Author_Id == id);
            _dbModel.Authors.Remove(author);
            _dbModel.SaveChanges();
            return RedirectToAction("Index");
        }
    }
}