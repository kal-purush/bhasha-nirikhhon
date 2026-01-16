using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;
using System.Text;
using System.Text.Encodings.Web;
using BMOS.Models.Entities;
using Bmostest.Services;

namespace TestDemoBmos.Controllers
{
    public class AccountController : Controller
    {
        private BmosContext _db = new BmosContext();
        private readonly UserManager<IdentityUser> _userManager;
        public IActionResult Login()
        {
            return View();
        }

        [HttpPost]
        public IActionResult Login(TblUser model)
        {
            //if (ModelState.IsValid){
            string username = model.Username;
            string password = model.Password;

            if (username != null || password != null)
            {
                var check = _db.TblUsers.Where(p => p.Username.Equals(username) && p.Password.Equals(password));
                if (check.Count() > 0)
                {
                    var checkConfirm = _db.TblUsers.Where(p => p.Username.Equals(username) && p.IsConfirm == true).Select(p => p.IsConfirm).ToList();
                    if (checkConfirm.Count() > 0)
                    {
                        HttpContext.Session.SetString("username", model.Username);
                        return RedirectToAction("Index", "Home");
                    }
                    ViewBag.EmailConfirm = "*Tài khoản của bạn chưa được kích hoạt, vui lòng kiểm tra Email để xác nhận tài khoản.";
                    return View();
                }
                else
                {
                    ViewBag.Notice = "*Tên đăng nhập hoặc mật khẩu không chính xác.";
                    return View();
                }
            }
            //}
            return View();
        }

        public IActionResult Logout()
        {
            HttpContext.Session.Remove("username");
            return RedirectToAction("Index", "Home");
        }
        public IActionResult Register()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> RegisterAsync(TblUser model)
        {

            var userId = model.Username;
            var code = "qwert";
            code = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(code));
            userId = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(userId));
            var content = Url.Action("ConfirmEmail", "Account", new { userId = userId, code = code }, protocol: Request.Scheme);
            var check = _db.TblUsers.FirstOrDefault(p => p.Username == model.Username);

            if (check == null)
            {
                _db.TblUsers.Add(model);
                await EmailSender.SendEmailAsync(model.Username, "Xác thực tài khoản", "<a href=\"" + content + "\" class=\"linkdetail\" style=\"text-decoration: none; margin: 0 auto; color: black;\">Kích hoạt tài khoản</a>");
                _db.SaveChanges();

                ViewBag.RegisterSuccess = "*Đăng ký tài khoản thành công, vui lòng kiểm tra ";
                return View();
            }
            else
            {
                ViewBag.Error = "*Email đã được đăng ký.";
                return View();
            }
        }

        public IActionResult ConfirmEmail(string userId, string code)
        {
            if (userId == null || code == null)
            {
                return RedirectToAction("Index");
            }
            userId = Encoding.UTF8.GetString(WebEncoders.Base64UrlDecode(userId));
            var check = _db.TblUsers.FirstOrDefault(p => p.Username == userId);
            if (check != null)
            {
                check.IsConfirm = true;
                _db.SaveChanges();
                ViewBag.Confirmed = "*Cảm ơn bạn đã đăng ký tài khoản.";
            }
            return View();
        }

        public IActionResult ForgotPassword()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> ForgotPasswordAsync(string username)
        {
            var userId = username;
            var code = "qwert";
            code = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(code));
            userId = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(userId));
            var content = Url.Action("ChangePassword", "Account", new { userId = userId, code = code }, protocol: Request.Scheme);
            var check = _db.TblUsers.FirstOrDefault(p => p.Username == username);

            if (check != null)
            {
                await EmailSender.SendEmailAsync(username, "Quên mật khẩu", "Please confirm your account by clicking <a href=\"" + content + "\">click here</a>");
                ViewBag.ConfirmForgotSuccess = "*Vui lòng kiểm tra ";
                return View();
            }
            else
            {
                ViewBag.ErrorConfirmForogot = "*Email chưa được đăng ký.";
                return View();
            }
        }



        public IActionResult ChangePassword(string userId, string code)
        {
            if (userId == null || code == null)
            {
                return RedirectToAction("Index");
            }
            userId = Encoding.UTF8.GetString(WebEncoders.Base64UrlDecode(userId));
            var check = _db.TblUsers.FirstOrDefault(p => p.Username == userId);
            if (check != null)
            {
                ViewBag.User = userId;
                return View();
            }
            return View();
        }

        [HttpPost]
        public IActionResult ChangePassword(TblUser user)
        {
            string username = user.Username;
            string newPassword = user.Password;

            var check = _db.TblUsers.FirstOrDefault(p => p.Username == username);
            if (check != null)
            {
                check.Password = newPassword;
                _db.SaveChanges();
                string changeSucces = "Thay đổi mật khẩu thành công";
            }
            return RedirectToAction("Login", "Account");
        }


    }
}