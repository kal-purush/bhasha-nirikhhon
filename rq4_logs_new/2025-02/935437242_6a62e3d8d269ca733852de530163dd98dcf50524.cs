using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using PortfolioProject.Data;
using PortfolioProject.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;

namespace PortfolioProject.Controllers
{
    public class SectionsController : Controller
    {
        public IActionResult HTMLIndex()
        {
            return View();
        }
        public IActionResult CSSIndex()
        {
            return View();
        }
        public IActionResult JavascriptIndex()
        {
            return View();
        }
    }
}
/*using System.IO;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Photolio.Data;
using PortfolioWebsite.Models;
using Microsoft.AspNetCore.Identity;
using PortfolioWebsite.Areas.Identity.Data;
using Microsoft.EntityFrameworkCore;

namespace PortfolioWebsite.Controllers
{
    public class PhotoController : Controller
    {
        private readonly ApplicationDbContext _context;

        private readonly IWebHostEnvironment _environment;
        
        private readonly IHttpContextAccessor _contextAccessor;

        private readonly UserManager<PhotolioUser> _userManager;

        public PhotoController(ApplicationDbContext context, IWebHostEnvironment Environment, IHttpContextAccessor contextAccessor, UserManager<PhotolioUser> userManager)
        { 
            _context = context;
            _environment = Environment;
            _contextAccessor = contextAccessor;
            _userManager = userManager;
        }

        [HttpGet]
        public async Task<IActionResult> Upload()
        {
            PhotolioUser user = await _userManager.GetUserAsync(User);
            if (user != null)
            {
                return View();
            }
            else
            {
                // Generic error message
                TempData["message"] = "You need to log in to Upload photos!";
                ViewBag.Message = TempData["message"];
                return View();
            }
        }

        [HttpPost]
        public async Task<IActionResult> Upload(List<IFormFile> postedFiles)
        {
            // Gets the necessary paths to add the files to the specified folder
            //string wwwPath = this._environment.WebRootPath;
            //string contentPath = this._environment.ContentRootPath;

            // Creates a path to the images folder in wwwroot
            string path = Path.Combine(this._environment.WebRootPath, "images");

            // If the image folder somehow doesn't exist, create it
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            // Creates a list to temporarily track uploaded Files
            List<string> uploadedFiles = new List<string>();

            // Gets current user
            PhotolioUser user = await _userManager.GetUserAsync(User);
            if (user == null)
            {
                // Generic error message
                TempData["message"] = "You need to log in to Upload photos!";
                ViewBag.Message = TempData["message"];
                return View();
            }

            // For each file posted on the Upload page
            foreach (IFormFile postedFile in postedFiles)
            {
                string userId = user.Id;

                // Creates the name of the current file
                string fileName = Path.GetFileName(postedFile.FileName);

                // Add the file to the database
                Photo photo = new Photo();
                photo.PhotoPath = fileName;
                photo.UserId = userId;
                _context.Add(photo);
                await _context.SaveChangesAsync();

                // Add the file to the image folder and return a success message
                using (FileStream stream = new FileStream(Path.Combine(path, fileName), FileMode.Create))
                {
                    postedFile.CopyTo(stream);
                    uploadedFiles.Add(fileName);
                    ViewBag.Message += string.Format("<b>{0}</b> uploaded.<br />", fileName);
                }
            }

            return View();
        }

        [HttpGet]
        public async Task<IActionResult> ViewPhotos(ViewPhotosViewModel model)
        {
            // Creates a new view model to pass to the View Photos page
            ViewPhotosViewModel vpvm = new ViewPhotosViewModel();

            // Gets current user and their userId
            PhotolioUser user = await _userManager.GetUserAsync(User);
            if (user != null)
            {
                string userId = user.Id;

                // Retrieves all photos from database and sends them 
                // to a list for processing
                List<Photo> photos = _context.Photos.ToList();

                // Creates a temporary list to keep track of photo 
                // paths that should be displayed in View Photos, 
                // is converted to an array later in the code
                List<Photo> list = new List<Photo>();

                // Checks the userId of the currently logged in 
                // user, and compares it to the userId of each 
                // photo object to check which photos belong to 
                // the user
                foreach (Photo photo in photos)
                {
                    if (userId == photo.UserId)
                    {
                        // Adds the photo to the list to be sent to the 
                        // View Photos page
                        list.Add(photo);
                    }
                }

                // Sends the list to the view model
                vpvm.Photos = list;

                // Sends the view model to the View Photos page
                return View(vpvm);
            }
            else
            {
                // Generic error message
                TempData["message"] = "You need to log in to View photos!";
                ViewBag.Message = TempData["message"];
                return View();
            }
        }

        [HttpGet]
        public async Task<IActionResult> Delete(int id)
        {
            // Gets the photo to delete by its PhotoId
            Photo? photoToDelete = await _context.Photos.FindAsync(id);

            // If null return an error
            if (photoToDelete == null)
            {
                return NotFound();
            }

            return View(photoToDelete);
        }

        [HttpPost, ActionName("Delete")]
        public async Task<IActionResult> DeleteConfirmation(int id)
        {
            // Gets the photo to delete by its PhotoId
            Photo? photoToDelete = await _context.Photos.FindAsync(id);

            // If there is a photo to delete
            if (photoToDelete != null)
            {
                // Removes the photo from the database
                _context.Photos.Remove(photoToDelete);
                await _context.SaveChangesAsync();

                // Creates a path to the wwwroot folder and then the current photo
                string path = Path.Combine(this._environment.WebRootPath, "images");
                string truePath = path + "/" + photoToDelete.PhotoPath;

                // Removes the photo from the wwwroot folder
                System.IO.File.Delete(truePath);

                TempData["Message"] = photoToDelete.PhotoPath + " was deleted successfully!";
                return RedirectToAction("ViewPhotos");
            }

            TempData["Message"] = "This was already deleted!";
            return RedirectToAction("ViewPhotos");
        }
    }
}
*/