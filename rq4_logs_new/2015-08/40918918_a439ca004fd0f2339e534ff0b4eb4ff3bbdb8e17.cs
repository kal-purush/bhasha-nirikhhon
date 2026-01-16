using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using GoodQRma.DAL;
using System.Web.Mvc;

namespace GoodQRma.Controllers
{
    public class FileController : Controller
    {

        private goodQRmaContext db = new goodQRmaContext();


        // GET: File
        public ActionResult Index(int id)
        {

            var fileToRetrieve = db.Files.Find(id);


            return File(fileToRetrieve.Content, fileToRetrieve.ContentType);
        }
    }
}