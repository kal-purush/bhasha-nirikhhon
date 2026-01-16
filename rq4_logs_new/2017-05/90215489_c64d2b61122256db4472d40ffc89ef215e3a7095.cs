using Microsoft.Web.WebPages.OAuth;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Routing;

namespace LinkHubUI
{ 
    public class OAuthConfig
    {
        public static void RegisterProviders()
        {           
            OAuthWebSecurity.RegisterGoogleClient(); 
            OAuthWebSecurity.RegisterFacebookClient( appId: ConfigurationManager.AppSettings["AppId"], appSecret: ConfigurationManager.AppSettings["AppSecret"]);
        }
    }
    public class MvcApplication : System.Web.HttpApplication
    {

        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            GlobalFilters.Filters.Add(new AuthorizeAttribute());

            OAuthConfig.RegisterProviders();
        }
    }
}