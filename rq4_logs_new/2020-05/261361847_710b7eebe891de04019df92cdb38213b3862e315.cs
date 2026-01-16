using System.IO;
using System.Threading.Tasks;
using MovieData.Models;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;

namespace MovieData.Controllers
{
    public class AddMovie2Controller : Controller
    {
        private readonly DatingAppContext _context;

        public AddMovie2Controller(DatingAppContext context)
        {
            _context = context;
        }

        [HttpGet]
        public ActionResult ViewPhoto(int id)
        {
            var photo = _context.MovieDetails.Find(id).Image;

            return File(photo, "image/jpeg"); // you'll need to specify the content type based on your picture
        }

        [HttpGet]
        public async Task<IActionResult> List()
        {
            return View(await _context.MovieDetails.ToListAsync());
        }

        [HttpGet]
        public IActionResult Index()
        {
            return View();
        }
        [HttpPost]
        public async Task<IActionResult> Index(MovieDetails movies, List<IFormFile> Image)
        {
            foreach (var item in Image)
            {
                if (item.Length > 0)
                {
                    using var stream = new MemoryStream();
                    await item.CopyToAsync(stream);
                    movies.Image = stream.ToArray();
                }
            }
            _context.MovieDetails.Add(movies);
            _context.SaveChanges();
            return View();
        }
    }
}