using DaBank.Models;
using DaBank.Repository;
using Microsoft.AspNetCore.Mvc;

namespace DaBank.Controllers
{
    public class TransferController : Controller
    {
        private readonly IBankRepository _repository;

        public TransferController(IBankRepository repository)
        {
            _repository = repository;
        }

        [HttpGet]
        public IActionResult Index()
        {
            var model = new Transaction();

            return View(model);
        }

        [HttpPost]
        public IActionResult Index(Transaction transaction)
        {
            var bankController = new BankController();
            var bank = _repository.GetBank();

            var accFrom = _repository.GetAccount(transaction.AccountFrom);
            var accTo = _repository.GetAccount(transaction.AccountTo);

            if (!(transaction.Amount > 0))
            {
                ViewBag.Error = "Please enter valid amount";
                return View(transaction);
            }

            if (accFrom == null || accTo == null)
            {
                ViewBag.Error = "Account cannot be found";
                return View(transaction);
            }

            if (!bankController.Transfer(accFrom, accTo, transaction.Amount))
            {
                ViewBag.Error = "Not enough funds";
                return View(transaction);
            }

            return View("Success", new Account[] { accFrom, accTo });
        }
    }
}