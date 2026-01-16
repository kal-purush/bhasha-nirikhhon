using IdeaCreator.Models.Abstract;
using IdeaCreator.Models.Entities;
using IdeaCreator.Models.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace IdeaCreator.Controllers
{
    public class IdeaController : Controller
    { 
        private IIdeasRepository repository;
        public int pageSize = 4;
        public IdeaController(IIdeasRepository repo)
        {
            repository = repo;
        }

        public ViewResult Ideas(int page = 1)
        {
            IdeaViewModel model = new IdeaViewModel
            {
                Ideas = repository.Ideas
                .OrderBy(idea => idea.ID)
                .Skip((page - 1) * pageSize)
                .Take(pageSize),
                PagingInfo = new PagingInfo
                {
                    CurrentPage = page,
                    ItemsPerPage = pageSize,
                    TotalItems = repository.Ideas.Count()
                }
            };

            return View(model);
        }

        public ViewResult Show(string id)
        {
            string a = id;

            IdeaViewModel model = new IdeaViewModel
            {
                Ideas = repository.Ideas
            };

            Idea res = model.Ideas.Where(x => x.ID == int.Parse(id)).First();


            return View(res);
        }
	}
}