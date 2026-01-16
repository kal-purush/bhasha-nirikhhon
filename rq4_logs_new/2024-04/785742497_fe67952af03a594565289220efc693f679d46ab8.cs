using Microsoft.AspNetCore.Mvc;

namespace UrlShortener.Controllers
{
    public class UrlController : Controller
    {
        [HttpGet]
        public IActionResult GetList()
        {
            return View();
        }

        [HttpGet]
        public IActionResult CreateUrl()
        {
            return View();
        }

        [HttpPost]
        public IActionResult CreateUrl(string url)
        {
            return View();
        }

        [HttpDelete]
        public IActionResult DeleteShortUrl(string redirectUrl)
        {
            return Redirect(redirectUrl);
        }
        
        [HttpGet]
        [Route("/{id}")]
        public IActionResult UseShortUrl(string id)
        {
            return Content("Ok");
        }
    }
}