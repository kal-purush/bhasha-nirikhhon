using System;
using System.Net;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Owin;

namespace IOUService
{
    class Startup
    {
        public void Configuration(IAppBuilder appBuilder)
        {
            HttpConfiguration configuration = new HttpConfiguration();

            configuration.Routes.MapHttpRoute(
                name:"DefaultRoute",
                routeTemplate:"api/{controller}/{action}",
                defaults: new { id = RouteParameter.Optional}
                );

            var listener = (HttpListener)appBuilder.Properties["System.Net.HttpListener"];
            listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication;

            appBuilder.UseWebApi(configuration);
        }
    }

    public class IOUController : ApiController
    {
        public string GetCurrentUserName()
        {
            Console.WriteLine(User.Identity.Name);
            return User.Identity.Name;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            using (WebApp.Start<Startup>(url: "http://+:4242/"))
            {
                Console.Write("IOU server running on port 4242");
                Console.ReadLine();
            }
        }
    }
}