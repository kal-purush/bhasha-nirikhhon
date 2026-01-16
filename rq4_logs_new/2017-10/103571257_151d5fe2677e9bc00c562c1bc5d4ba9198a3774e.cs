using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Mvc;
using FoodProject.Models;
namespace FoodProject.infrastructure.Binders
{
    public class UserModelBinder : IModelBinder
    {
        private const string sessionKey = "User";

        public object BindModel(ControllerContext controllerContext, ModelBindingContext bindingConetext)
        {
            User user = null;
            if (controllerContext.HttpContext.Session != null)
            {
                user = (User)controllerContext.HttpContext.Session[sessionKey];
            }
            if (user == null)
            {
                user = new User();
                if (controllerContext.HttpContext.Session != null)
                {
                    controllerContext.HttpContext.Session[sessionKey] = user;
                }
            }
            return user;
        }
    }
}