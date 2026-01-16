using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using ToyWebsite.DAL;
using ToyWebsite.Models;

namespace ToyWebsite.Controllers
{
    public class LoggingFilterAttribute : ActionFilterAttribute
    {
        public override void OnActionExecuting(ActionExecutingContext filterContext)
        {
            StoreContext context = new StoreContext();

            //Create a guest user
            string userNameHashKey = "GuestName" + HttpContext.Current.Request.UserHostAddress.ToString();
            string userEmailHashKey = "GuestEmail" + HttpContext.Current.Request.UserHostAddress.ToString();
            string userPasswordHashKey = "GuestPassword" + HttpContext.Current.Request.UserHostAddress.ToString();
            string guestName = (userNameHashKey.GetHashCode()).ToString();
            string guestEmail = (userNameHashKey.GetHashCode()).ToString() + "@" +
                                (userEmailHashKey.GetHashCode()).ToString() + ".com";
            string guestPassword = (userPasswordHashKey.GetHashCode()).ToString();
            User guestUser = new User
            {
                userName = guestName,
                userPassword = guestPassword,
                userConfirmPassword = guestPassword,
                userEmail = guestEmail,
                userGuest = true
            };

            //If the guest user bool is null we need to instantiate the user and as a guest
            if (HttpContext.Current.Session["GuestUser"] == null)
            {
                //Check if guest already exists in the DB
                bool guestAccountExists = (from u in context.Users
                                           where u.userName == guestName
                                           select u).Any();
                if (!guestAccountExists)
                {
                    context.Users.Add(guestUser);
                    context.SaveChanges();
                }
                else
                {
                    guestUser = (from u in context.Users
                                 where u.userName == guestName
                                 select u).Single();
                }

                HttpContext.Current.Session["GuestUser"] = true;
                HttpContext.Current.Session["UserID"] = guestUser.userID;

            }         
        }
    }
}