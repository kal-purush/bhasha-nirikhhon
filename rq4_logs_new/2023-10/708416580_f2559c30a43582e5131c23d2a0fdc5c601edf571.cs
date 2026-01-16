using Microsoft.AspNetCore.Mvc;
using BookWebEcommerce.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Mvc.Rendering;
using BookWebEcommerce.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using X.PagedList;

namespace BookWebEcommerce.Controllers
{
    public class TranslatorController : Controller
    {
        private AppDbContext db;
        public TranslatorController(AppDbContext context)
        {
            db = context;
        }
        public IActionResult Index(int? page)
        {
            int pageSize = 3;
            int pageNumber= page==null || page<0 ? 1 : page.Value;
            var list = db.Translators.AsNoTracking().OrderBy(x => x.Name);
            PagedList<Translator> lst= new PagedList<Translator>(list,pageNumber,pageSize);
            return View(lst);
           // var translators = db.Translators.ToList();
            //return View(translators);
        }
        public long insert(Translator model)
        {
            db.Translators.Add(model);
            db.SaveChanges();
            return model.Id;
        }
        
        public IActionResult Create()
        {
            return View();
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        //[Bind("Name,Age,Nationality,Genre,Bio")]
        public IActionResult Create(Translator translator)
        {
            if (!ModelState.IsValid)
            {
                long id = insert(translator);
                if (id > 0)
                {
                    return RedirectToAction("Index");
                }
                else
                {
                    ModelState.AddModelError("", "Fail");
                }

            }
            return View();
        }
        public int Edit1(Translator model)
        {
            try
            {
                var dt = db.Translators.Find(model.Id);
                if (db.Translators.FirstOrDefault(x => x.Id == model.Id) != null)
                {
                    dt.Name = model.Name;
                    dt.Age = model.Age;
                    dt.Nationality = model.Nationality;
                    dt.Genre = model.Genre;
                    dt.Bio = model.Bio;
                    db.SaveChanges();
                    return 0;
                }
                else
                {
                    return 1;
                }

            }
            catch (Exception ex)
            { return 2; }
        }
        [HttpGet]
        public IActionResult Edit(int id)
        {
            //if (id==null || db.Translators == null)
            //    {
            //        return NotFound();
            //    }
            //    var translator = db.Translators.Find(id);
            //    if (translator == null)
            //    {
            //        return NotFound();
            //    }
            var result = db.Translators.Find(id);
            return View(result);

        }
        [HttpPost]
        public IActionResult Edit(Translator translator)
        {
            //if (id!= translator.Id)
            //{
            //    return NotFound();
            //}
            //if (ModelState.IsValid)
            //{
            //    try
            //    {
            //        db.Update(translator);
            //        db.SaveChanges();
            //    }
            //    catch(DbUpdateConcurrencyException)
            //    {
            //        if(!TranslatorExists(translator.Id))
            //        {
            //            return NotFound();
            //        }
            //        else
            //        {
            //            throw;
            //        }

            //    }
            //    return RedirectToAction("Index");
            //}
            var result = Edit1(translator);
            if (result == 0)
            {
                return RedirectToAction("Index");
            }
            else if (result == 1)
            {
                ModelState.AddModelError("", "Chỉnh sửa bị trùng ");
            }
            else { ModelState.AddModelError("", "Chỉnh sửa không chính xác "); }
            return View();
        }
        private bool TranslatorExists(int id)
        {
            return (db.Translators?.Any(e => e.Id == id)).GetValueOrDefault();
        }

        public IActionResult Delete(int id)
        {
            if (id == null || db.Translators == null)
            {
                return NotFound();
            }
            var translator = db.Translators.FirstOrDefault(m => m.Id == id);
            if (translator == null)
            {
                return NotFound();
            }
            return View(translator);
        }
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public IActionResult DeleteConfirmed(int id)
        {
            if (db.Translators == null)
            {
                return Problem("Enity set 'Translator' is null");
            }
            var translator = db.Translators.Find(id);
            if (translator != null)
            {
                db.Translators.Remove(translator);
            }
            db.SaveChanges();
            return RedirectToAction("Index");
        }
    }
}