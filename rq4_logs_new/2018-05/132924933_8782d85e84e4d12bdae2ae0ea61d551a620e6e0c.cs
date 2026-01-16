using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Openlogin.Models;
using System.DirectoryServices;

namespace Openlogin.Controllers
{
    public class AccountController : Controller
    {
        // GET: Account
        public ActionResult Index()
        {
            return View();
        }


        public ActionResult ChangePassword()
        {
            var user = new ChangePassword();
            var userID = Request.LogonUserIdentity.Name.Substring(Request.LogonUserIdentity.Name.LastIndexOf(@"\") + 1);
            user.Username = userID;
            return View(user);
        }

        [HttpPost]
        public ActionResult ChangePassword(ChangePassword account)
        {
            if (ModelState.IsValid)
            {
                String machineAndUser = Environment.MachineName + "\\" + account.Username;
                string adsPath = String.Format("WinNT://{0}/{1}, user", Environment.MachineName, account.Username);
                DirectoryEntry user = new DirectoryEntry(adsPath, machineAndUser, account.OldPassword, AuthenticationTypes.Secure);

                try
                {
                    user.Invoke("ChangePassword", new object[] { account.OldPassword, account.NewPassword });
                    ViewBag.Message = "Password change successful";
                }
                catch (Exception ex)
                {
                    string errMessage = ex.Message;
                    while ((ex = ex.InnerException) != null)
                    {
                        errMessage = ex.Message;
                    }
                    ViewBag.Message = errMessage;
                }
                finally
                {
                    user.Dispose();
                }
                //ModelState.Clear();
            }
            return View();
        }
    }
}