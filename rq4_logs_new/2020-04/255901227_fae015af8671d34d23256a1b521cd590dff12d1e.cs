using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using NeedyBuddy.Data;
using NeedyBuddy.Data.Model;
using NeedyBuddy.Models;

namespace NeedyBuddy.Controllers
{
    public class MyServicesController : Controller
    {
        private readonly ApplicationDbContext _context;
        private readonly IRepository _repository;


        //private Task<ApplicationUser> GetCurrentUserAsync() => _userManager.GetUserAsync(HttpContext.User);
        public MyServicesController(ApplicationDbContext context, IRepository repository)
        {
            _context = context;
            _repository = repository;


        }
        
        public ViewResult Index()
        {
            return View();

        }
        public ViewResult MyServicesDetails()
        {
            var c = _context.Users;
            var currentUserName = User.Identity.Name;
            var userServicesViewModel = from p in _context.Users
                                        join q in _context.Service on p.Id equals q.User.Id
                                        //join r in _context.ServiceCategory on q.ServiceCategory.ServiceCategoryId equals r.ServiceCategoryId
                                        join r in _context.ServiceCategory on q.ServiceCategory.ServiceCategoryId equals r.ServiceCategoryId
                                        where p.UserName.Equals(currentUserName)
                                        select new UserServicesViewModel
                                        {
                                            Id = p.Id,
                                            UserName = p.UserName,
                                            ContactNumber = p.PhoneNumber,
                                            Email = p.Email,
                                            City = p.City,
                                            Pincode = p.Pincode,
                                            ServiceName = r.ServiceName,
                                            Descriptions = q.Descriptions
                                        };
            return View(userServicesViewModel.ToList());
        }

    }
}