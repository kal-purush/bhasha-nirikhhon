using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BankApp.Repository;
using Microsoft.AspNetCore.Mvc;

namespace BankApp.Controllers
{
    public class ClientsController : Controller
    {
        private readonly HomeRepository homeRepository;

        public ClientsController(HomeRepository _homeRepository)
        {
            homeRepository = _homeRepository;
        }
        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Balance(string ID)
        {
            var bal = homeRepository.ClientBalance(ID);
            return View(bal);
        }
    }
}