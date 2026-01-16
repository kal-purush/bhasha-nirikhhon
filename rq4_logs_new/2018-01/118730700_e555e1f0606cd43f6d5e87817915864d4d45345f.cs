using AccountExternalFunction;
using AccountExternalModel;
using ExternalAccountWebAuthentication.Authentication;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.Mvc;
using System.Web.Security;

namespace AndersonNotificationWeb.Controllers
{
    [MvcAuthorizationFilter(true)]
    public class CredentialController : BaseController
    {
        private IFCredential _iFCredential;
        public CredentialController(IFCredential iFCredential)
        {
            _iFCredential = iFCredential;
        }


        #region Read
        [Route("Login")]
        [HttpGet]
        public ActionResult Login()
        {
            try
            {
                return View();
            }
            catch (Exception exception)
            {
                return Json(exception.ToString());
            }
        }

        [Route("Login")]
        [HttpPost]
        public ActionResult Login(Credential credential)
        {
            try
            {
                credential = _iFCredential.Login(credential);
                bool isLogin = credential.CredentialId > 0;
                if (isLogin)
                {
                    string encryptedUsername = Convert.ToBase64String(MachineKey.Protect(Encoding.UTF8.GetBytes(credential.Username)));
                    string encryptedId = Convert.ToBase64String(MachineKey.Protect(Encoding.UTF8.GetBytes(credential.CredentialId.ToString())));
                    HttpCookie credentialCookies = new HttpCookie("Credential");
                    credentialCookies["Username"] = encryptedUsername;
                    credentialCookies["CredentialId"] = encryptedId;
                    credentialCookies.Expires = DateTime.Now.AddHours(24);
                    Response.Cookies.Add(credentialCookies);
                    return Redirect("~/Home");
                }
                return View();
            }
            catch (Exception exception)
            {
                return Json("Error on logging in");
            }
        }

        [HttpGet]
        public ActionResult Signout()
        {
            HttpCookie credentialCookies = new HttpCookie("Credential");
            credentialCookies.Expires = DateTime.Now.AddHours(-1);
            Response.Cookies.Add(credentialCookies);
            return Redirect("~/Credential/Login");
        }
    }
    #endregion
}