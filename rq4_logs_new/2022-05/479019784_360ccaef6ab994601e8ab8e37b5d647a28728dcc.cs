
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.IdentityModel.Protocols;
using ResidentSleeper.Contexts;
using ResidentSleeper.Models;
using ResidentSleeper.Services.JWTService;
using ResidentSleeper.Services.UserService;
using System;
using System.Configuration;
using System.Web;

namespace ResidentSleeper.Attributes
{
    public class AuditAttribute : ActionFilterAttribute
    {
        public override void OnActionExecuting(ActionExecutingContext filterContext)
        {
            var useAudit = Convert.ToBoolean(ConfigurationManager.AppSettings["AttributeAudit"]);
            Console.WriteLine(useAudit);
            if (!useAudit) return;

            var request = filterContext.HttpContext.Request;
            string controllerName;
            string actionName;
            string ipAddress = "000.000.000.000";

            try
            {
                controllerName = ((ControllerBase)filterContext.Controller).ControllerContext.ActionDescriptor.ControllerName;
                actionName = ((ControllerBase)filterContext.Controller).ControllerContext.ActionDescriptor.ActionName;
                ipAddress = ((ControllerBase)filterContext.Controller).ControllerContext.HttpContext.Connection.RemoteIpAddress.ToString();
            }
            catch (Exception)
            {
                controllerName = ((Controller)filterContext.Controller).ControllerContext.ActionDescriptor.ControllerName;
                actionName = ((Controller)filterContext.Controller).ControllerContext.ActionDescriptor.ActionName;
                ipAddress = ((Controller)filterContext.Controller).ControllerContext.HttpContext.Connection.RemoteIpAddress.ToString();
            }

            var username = "Anonymous";

            //Generate an audit
            Audit audit = new Audit()
            {
                //Your Audit Identifier     
                AuditID = Guid.NewGuid(),
                //Our Username (if available)
                UserName = username,
                //The IP Address of the Request
                IPAddress = ipAddress,
                //The URL that was accessed
                AreaAccessed = ($"{controllerName}/{actionName}"),
                //Creates our Timestamp
                Timestamp = DateTime.UtcNow
            };

            //Stores the Audit in the Database
            var context = filterContext.HttpContext.RequestServices.GetService(typeof(MainContext)) as MainContext;
            context.AuditRecords.Add(audit);
            context.SaveChanges();

            //Finishes executing the Action as normal 
            base.OnActionExecuting(filterContext);
        }
    }
}