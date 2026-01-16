using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace techmusings_api.Controllers
{
    class Post
    {
        public string date;
        public string title;
        [JsonProperty(PropertyName = "abstract")] public string summary;
        public string picture;
        public string link;
    }

    [Route("api/[controller]")]
    public class PostsController : Controller
    {
        [HttpGet]
        public JsonResult Get()
        {
            Post[] result = {
                new Post
                {
                    date = "April 28, 2017",
                    title = "CI/CD inside Docker 3",
                    summary = "Aenean ornare velit lacus varius enim ullamcorper proin aliquam facilisis ante sed etiam magna interdum congue. Lorem ipsum dolor amet nullam sed etiam veroeros.",
                    picture = "/images/ship1.jpg",
                    link = "/article/docker-3"
                }, new Post
                {
                    date = "April 24, 2017",
                    title = "CI/CD inside Docker 2",
                    summary = "Aenean ornare velit lacus varius enim ullamcorper proin aliquam facilisis ante sed etiam magna interdum congue. Lorem ipsum dolor amet nullam sed etiam veroeros.",
                    picture = "/images/pic02.jpg",
                    link = "/article/docker-2"
                }, new Post
                {
                    date = "April 22, 2017",
                    title = "CI/CD inside Docker 1",
                    summary = "Aenean ornare velit lacus varius enim ullamcorper proin aliquam facilisis ante sed etiam magna interdum congue. Lorem ipsum dolor amet nullam sed etiam veroeros.",
                    picture = "/images/pic02.jpg",
                    link = "/article/docker-1"
                }};
            return Json(result);
        }
    }
}