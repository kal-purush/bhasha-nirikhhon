using HANGOSELL_KLTN.Models.EF;
using HANGOSELL_KLTN.Service;
using Microsoft.AspNetCore.Mvc;

namespace HANGOSELL_KLTN.Areas.Admin.Controllers
{
    [Area("Admin")]
    public class BankAccountController : Controller
    {
        private readonly BankAccountService bankAccountService; // Dịch vụ tài khoản ngân hàng

        public BankAccountController(BankAccountService bankAccountService)
        {
            this.bankAccountService = bankAccountService;
        }

        public IActionResult Index()
        {
            ViewData["EmployeeName"] = HttpContext.Session.GetString("EmployeeName");
            ViewData["Avatar"] = HttpContext.Session.GetString("Avatar");
            ViewData["Position"] = HttpContext.Session.GetString("Position");
            List<ModelDataset> ModelDatasets = new List<ModelDataset>();
            List<QRCodeRequest> bankAccounts = bankAccountService.GetAllBankAccounts(); // Lấy danh sách tài khoản ngân hàng
            foreach (QRCodeRequest bankAccount in bankAccounts)
            {
                ModelDataset dataset = new ModelDataset()
                {
                    qRCodeRequest = bankAccount,
                };
                ModelDatasets.Add(dataset);
            }

            return View(ModelDatasets); // Trả về view với danh sách tài khoản ngân hàng
        }

        public IActionResult Create()
        {
            ViewData["EmployeeName"] = HttpContext.Session.GetString("EmployeeName");
            ViewData["Avatar"] = HttpContext.Session.GetString("Avatar");
            ViewData["Position"] = HttpContext.Session.GetString("Position");
            return View();
        }

        public IActionResult Create(QRCodeRequest newBankAccount)
        {
            if (ModelState.IsValid)
            {
                bankAccountService.AddBankAccount(newBankAccount); // Thêm tài khoản mới
                return RedirectToAction(nameof(Index)); // Quay lại trang danh sách tài khoản
            }
            return View(newBankAccount);
        }

        public IActionResult Edit(int id)
        {
            ViewData["EmployeeName"] = HttpContext.Session.GetString("EmployeeName");
            ViewData["Avatar"] = HttpContext.Session.GetString("Avatar");
            ViewData["Position"] = HttpContext.Session.GetString("Position");

            QRCodeRequest bankAccount = bankAccountService.GetBankAccountById(id); // Lấy tài khoản theo ID
            if (bankAccount == null)
            {
                return NotFound();
            }
            return View(bankAccount);
        }

        public IActionResult EditQRCodeRequest(QRCodeRequest updatedBankAccount)
        {
            updatedBankAccount.Template = "null";
            bankAccountService.UpdateBankAccount(updatedBankAccount); // Cập nhật tài khoản
            return Redirect("/Admin/Store");

        }

        public IActionResult DeleteBankAccount(int id)
        {
            bankAccountService.DeleteBankAccount(id);
            return Json(new { success = true });
        }

    }
}