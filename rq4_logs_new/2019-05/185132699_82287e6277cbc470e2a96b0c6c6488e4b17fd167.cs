using o2o.entity;
using o2o.Utils;
using Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.SessionState;

namespace o2o.Controller
{
    /// <summary>
    /// Summary description for index
    /// </summary>
    public class index : IHttpHandler,IRequiresSessionState
    {
        UserService userService = new UserService();
        public void ProcessRequest(HttpContext context)
        {
            User user = new User();
            Dictionary<String,Object> dictionary = new Dictionary<string,object>();
            String username = context.Request["username"];
            String password = context.Request["password"];
            user = userService.userLogin(username, password);
            if (user!=null)
            {
                context.Session["userId"] = user.Id;
                context.Session["nickname"] = user.NickName;
                dictionary.Add("success", "true");
            }
            else
            {
                dictionary.Add("success", "false");
            }
            StringBuilder sb = JsonUtil.toJson(dictionary);
            context.Response.Write(sb.ToString());
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