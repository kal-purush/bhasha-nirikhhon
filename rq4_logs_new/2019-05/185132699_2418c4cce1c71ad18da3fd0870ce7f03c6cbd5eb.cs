using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace o2o.Controller
{
    /// <summary>
    /// Summary description for register
    /// </summary>
    public class register : IHttpHandler
    {

        public void ProcessRequest(HttpContext context)
        {
            context.Response.ContentType = "text/plain";
            
            String name = context.Request.Form["username"];
            context.Response.Write(name);
        }

        public bool IsReusable
        {
            get
            {
                return false;
            }
        }
    }
}