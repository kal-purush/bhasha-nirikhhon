using System.Web.Mvc;
using System.Web.Routing;

namespace Spring.Core.CDH.Autowire
{
    public class AutowireControllerFactory : DefaultControllerFactory
    {
        private string rootContextName;
        private AutowireAttribute controllerAutowireAttribute;

        public AutowireControllerFactory(string contextName = SpringAutowire.DefaultRootContextName)
        {
            rootContextName = contextName;
            controllerAutowireAttribute = new AutowireAttribute { Singleton = false };
        }

        public override IController CreateController(RequestContext requestContext, string controllerName)
        {
            var type = base.GetControllerType(requestContext, controllerName);
            return (IController)SpringAutowire.Autowire(type, controllerAutowireAttribute, rootContextName);
        }
    }
}