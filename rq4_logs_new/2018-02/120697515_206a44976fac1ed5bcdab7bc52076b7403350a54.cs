using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using System.Web.Hosting;
using System.Web.Mvc;

namespace TutorialForStorage.Controllers
{
    public class BrowserUploadController : Controller
    {
        private string UPLOAD_PATH = @"~/TempImages/";

        // GET: BrowserUpload
        public ActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<ActionResult> BrowserUploadMultipleFiles(IEnumerable<HttpPostedFileBase> files)
        {
            var folderPath = HostingEnvironment.MapPath(UPLOAD_PATH);

            //save each file posted by HTML5 browser to disk UPLOAD_PATH
            foreach (var file in files)
            {
                if (file != null && file.ContentLength > 0)
                {
                    file.SaveAs(Path.Combine(folderPath, file.FileName));
                }
            }

            var fileCount = files.Count<HttpPostedFileBase>();

            if (fileCount > 0)
            {
                var bc = new BlobsController();

                var folder = Directory.GetFiles(folderPath);

                //upload all files in UPLOAD_PATH to blob storage container
                //and then clean up by deleting
                foreach (var file in folder)
                {
                    await bc.UploadFile(file);

                    System.IO.File.Delete(file);
                }

                //set message for View to be returned
                if (fileCount == 1)
                {
                    ViewBag.Message = $"Uploaded {fileCount} file.";
                }
                else
                {
                    ViewBag.Message = $"Uploaded {fileCount} files.";
                }
            }
            else
            {
                ViewBag.Message = $"No files to upload.";
            }

            return View();
        }

    }
}