using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ALMBankRobertT.App.Models;
using ALMBankRobertT.App.Models.ViewModels;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling MVC for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ALMBankRobertT.App.Controllers
{
    public class TransferController : Controller
    {
        public IActionResult Index(TransferViewModel model = null)
        {
            if (model == null)
            {
                model = new TransferViewModel();
            }
            return View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Transfer(TransferViewModel model)
        {
            if (ModelState.IsValid)
            {
                var accounts = BankRepository.GetAccounts();
                var from = accounts.Find(x => x.AccountId == model.TransferFromId);
                var to = accounts.Find(x => x.AccountId == model.TransferToId);
                Account result = new Account();
                result.Transfer(model.Amount, from, to);
                model.ErrorMessage = result.ErrorMessage;
                model.SuccessMessage = result.SuccessMessage;
            }
            return View("Index", model);
        }
    }
}