using ITCompany.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace ITCompany.Controllers
{
    public class ImagesController : Controller
    {
        private static ImagesRepo images;

        static ImagesController()
        {
            images = new ImagesRepo();
        }
        // GET: Image
        public ActionResult ClientsGet()
        {
            return File(images.ClientsGet(), "image/jpeg");
        }

        [HttpPost]
        public ActionResult ClientsUpload(HttpPostedFileBase uploaded)
        {
            byte[] buffer = new byte[uploaded.ContentLength];
            uploaded.InputStream.Read(buffer, 0, buffer.Length);

            images.Update(buffer);
            return RedirectToAction("Index", "Client");
        }

        public ActionResult EmployeesGet()
        {
            return File(images.EmployeesGet(), "image/jpeg");
        }

        [HttpPost]
        public ActionResult EmployeesUpload(HttpPostedFileBase uploaded)
        {
            byte[] buffer = new byte[uploaded.ContentLength];
            uploaded.InputStream.Read(buffer, 0, buffer.Length);

            images.Update(buffer);
            return RedirectToAction("Index", "Employee");
        }

        public ActionResult ProjectsGet()
        {
            return File(images.ProjectsGet(), "image/jpeg");
        }

        [HttpPost]
        public ActionResult ProjectsUpload(HttpPostedFileBase uploaded)
        {
            byte[] buffer = new byte[uploaded.ContentLength];
            uploaded.InputStream.Read(buffer, 0, buffer.Length);

            images.Update(buffer);
            return RedirectToAction("Index", "Project");
        }
    }
}