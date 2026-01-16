using AutoMapper;
using DAL;
using DAL.Entities;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ProgrammingForum_ASPNETCore.Models.TopicModels;
using ProgrammingForum_ASPNETCore.Models.UserModels;

namespace ProgrammingForum_ASPNETCore.Controllers
{
    public class UserController : Controller
    {
        private readonly IMapper _mapper;

        public UserController(IMapper mapper)
        {
            _mapper = mapper;
        }


        [Authorize(Roles = "Admin")]
        public IActionResult AllUsers()
        {
            IEnumerable<User> users = null;

            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri("http://localhost:41696/api/");
                //HTTP GET
                var responseTask = client.GetAsync("User");
                responseTask.Wait();

                var result = responseTask.Result;
                if (result.IsSuccessStatusCode)
                {
                    var readTask = result.Content.ReadFromJsonAsync<IEnumerable<User>>();
                    readTask.Wait();
                    users = readTask.Result;
                }
                else //web api sent error response 
                {
                    //log response status here..
                    ModelState.AddModelError(string.Empty, "Server error. Please contact administrator.");
                }
            }

            var userViews = _mapper.Map<IEnumerable<UserViewModel>>(users);
            return View(userViews);
        }
    }
}