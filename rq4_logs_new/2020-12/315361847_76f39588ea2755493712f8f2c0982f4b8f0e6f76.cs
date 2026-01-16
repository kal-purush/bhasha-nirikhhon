using System.Collections.Generic;
using BookLib.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling MVC for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace BookLib.Controllers
{
    [Route("api")] 
    [ApiController]
    //RootController是api的一个入口点，通过调用/api可以发现author相关api，从而进行各种操作
    public class RootController : Controller
    {
        [HttpGet(Name = nameof(GetRoot))]
        public IActionResult GetRoot()
        {
            var links = new List<Link>();
            links.Add(new Link(HttpMethods.Get, "self", Url.Link(nameof(GetRoot),null)));
            links.Add(new Link(HttpMethods.Get, "get authors", Url.Link(nameof(AuthorController.GetAuthorsAsync), null)));
            links.Add(new Link(HttpMethods.Post, "create a author", Url.Link(nameof(AuthorController.CreateAuthorAsync), null)));

            return Ok(links);
        }
    }
}