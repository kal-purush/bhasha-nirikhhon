using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DaBank.Models;
using Microsoft.AspNetCore.Mvc;

namespace DaBank.Controllers
{
    public class BankController : Controller
    {
        public IActionResult Index()
        {
            return View();
        }

        public BankRepository CreateBankRep()
        {
            var bankRep = new BankRepository();

            bankRep.Customers = new List<Customer>
            {
                new Customer("David Agdelius", new List<Account>
                {
                    new Account(1, 10000)
                }),
                new Customer("Thorleif Svensson", new List<Account>
                {
                    new Account(2, 5000)
                }),
                new Customer("Ronny Karlsson", new List<Account>
                {
                    new Account(3, 5000000)
                }),
                new Customer("Eskil Knutsson", new List<Account>
                {
                    new Account(1, 100)
                })
            };

            return bankRep;
        } 
    }
}