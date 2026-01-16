using System;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Clothing_Store.Areas.Admin.Controllers
{
    public class BaseController : Controller
    {
        protected int GetCurrentUserID()
        {
            try
            {
                return (int)HttpContext.Session.GetInt32("admin_id");
            }
            catch (Exception e)
            {
                return -1;
            }
        }

        protected string GetCurrentUserName()
        {
            try
            {
                var username = HttpContext.Session.GetString("user_name");
                if (string.IsNullOrEmpty(username))
                {
                    return "Admin";
                }
                return username;
            }
            catch (Exception e)
            {
                return null;
            }
        }
    }
}