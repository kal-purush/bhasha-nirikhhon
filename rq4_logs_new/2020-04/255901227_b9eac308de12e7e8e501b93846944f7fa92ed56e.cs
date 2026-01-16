using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using NeedyBuddy.Models;
using NeedyBuddy.Data.Model;

namespace NeedyBuddy.Controllers
{
    public class ProfileController : Controller
    {
        public UserServicesViewModel loggedinUser = new UserServicesViewModel { Id = "1", UserName = "Chandan K", Email = "cksenapati05@gmail.com", ContactNumber = "8763717165", City = "Gachibowli", Descriptions = "S/W Developer", Pincode = "500032", ServiceName = "Medicine" };
        public List<ServiceCategory> servicesList = new List<ServiceCategory>();
        public List<ServiceCategory> selectedServicesList = new List<ServiceCategory>();
        public List<ServiceCategory> nonSelectedServicesList = new List<ServiceCategory>();


        public IActionResult Index()
        {

            getServicesList();
            ViewBag.servicesList = servicesList;
            ViewBag.selectedServicesList = selectedServicesList;
            ViewBag.nonSelectedServicesList = nonSelectedServicesList;


            ViewBag.loggedinUser = loggedinUser;
            return View();
        }

        public void getServicesList()
        {
            servicesList.Add(new ServiceCategory() { ServiceCategoryId = 1, ServiceName = "Test" });
            servicesList.Add(new ServiceCategory() { ServiceCategoryId = 2, ServiceName = "Food" });
            servicesList.Add(new ServiceCategory() { ServiceCategoryId = 3, ServiceName = "Medicine" });
            servicesList.Add(new ServiceCategory() { ServiceCategoryId = 4, ServiceName = "Grocessary" });
            servicesList.Add(new ServiceCategory() { ServiceCategoryId = 5, ServiceName = "Doctor" });
            servicesList.Add(new ServiceCategory() { ServiceCategoryId = 6, ServiceName = "Physical Help" });
            servicesList.Add(new ServiceCategory() { ServiceCategoryId = 7, ServiceName = "Transportation" });

            selectedServicesList.Add(new ServiceCategory() { ServiceCategoryId = 3, ServiceName = "Medicine" });

            foreach (ServiceCategory eachService in servicesList)
            {
                if (!selectedServicesList.Exists(x => x.ServiceCategoryId == eachService.ServiceCategoryId))
                {
                    nonSelectedServicesList.Add(eachService);
                }
                
            }
        }
    }
}