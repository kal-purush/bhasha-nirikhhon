using System.Web.Mvc;

namespace MVCWebApp.Areas.Admin
{
    public class AdminAreaRegistration : AreaRegistration 
    {
        public override string AreaName 
        {
            get 
            {
                return "Admin";
            }
        }

        public override void RegisterArea(AreaRegistrationContext context) 
        {
            context.MapRoute(
                name: "Admin_default",
                url: "Admin/{controller}/{action}/{id}",
                defaults: new { area = "Admin", controller = "Home", action = "Index", id = UrlParameter.Optional },
                namespaces: new[] { "MVCWebApp.Areas.Admin.Controllers" } // zodat ie in de juiste map kijkt, stackoverflow.com/questions/7842293
            );

            // Interesting links:
            // http://stackoverflow.com/questions/8926404/mvc3-razor-error-the-view-or-its-master-was-not-found-or-no-view-engine-support
            // Explains that localhost/products gets in the right controller but will never find the view. Instead (of course) use
            // localhost/admin/products

        }
    }
}