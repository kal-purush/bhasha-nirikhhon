using System.Web.Optimization;

namespace WebshopProject.App_Start
{
    public class BundleConfig
    {
        public static void RegisterBundles(BundleCollection bundles)
        {

            ScriptBundle thirdPartyScripts = new ScriptBundle("~/Scripts/ThirdParty");
            thirdPartyScripts.Include("~/Scripts/jquery-{version}.js")
                                .Include("~/Scripts/bootstrap.min.js")
                                .Include("~/Scripts/validate.min.js");

            bundles.Add(thirdPartyScripts);
            BundleTable.EnableOptimizations = true;
        }
    }
}