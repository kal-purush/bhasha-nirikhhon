using System.Web;
using System.Web.Optimization;

namespace ElysiumMagtheridon
{
    public class BundleConfig
    {
        public static void RegisterBundles(BundleCollection bundles)
        {
            bundles.Add(new StyleBundle("~/Content/css")
                         .Include("~/Content/Application/*.css"));

            bundles.Add(new ScriptBundle("~/Content/js")
                         .Include("~/Scripts/Application/*.js"));
        }
    }
}