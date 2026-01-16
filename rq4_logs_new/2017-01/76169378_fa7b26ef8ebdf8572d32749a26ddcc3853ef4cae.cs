using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Workflow_Models.Models;
using Workflow_BL.BSL;
using Workflow_BL.BL;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using System.IO;

// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace Workflow.Controllers
{
    public class WorkZoneController : Controller
    {

        private IHostingEnvironment _environment;

        public WorkZoneController(IHostingEnvironment environment)
        {
            _environment = environment;
        }

        // GET: /<controller>/
        public IActionResult Index()
        {
            return View();
        }
        
        // TOdo : when a file is uploaded check if already exists in db
        //          if yes -> uploade else add
        [HttpPost]
        public async Task<IActionResult> AddFile(DocumentModel model)
        {
            if(ModelState.IsValid)
            {
                var upload = Path.Combine(_environment.WebRootPath, "documents");
                var path = Path.Combine(upload, model.File.FileName);
                if(model.File.Length>0)
                    using (var fileStream = new FileStream
                        (Path.Combine(upload, model.File.FileName), FileMode.Create))
                    {
                        await model.File.CopyToAsync(fileStream);
                    }
                if (!object.Equals(ContributorService.GetDocumentByName(model.File.FileName), default(Document)))
                {

                    //ContributorService.ModifyDocument(model.File.FileName, model.File.FileName.Split('.')[1],
                    //    model.Metadata, path);
                }
                else
                {
                    //ContributorService.AddDocument(model.File.FileName, model.File.FileName.Split('.')[1],
                    //    model.Metadata, path);
                }
            }

            return View();
        }
    }
}