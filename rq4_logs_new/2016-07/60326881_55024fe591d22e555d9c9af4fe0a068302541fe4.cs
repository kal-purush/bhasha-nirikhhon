using Spring.Context;
using Spring.Context.Support;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;
using System.Web.Routing;
using System.Web.SessionState;

namespace ArchitectureFrame.Web.Agency
{

    //对Controller依赖注入需要新建一个ControllerFactory。
    //我们实现System.Web.Mvc.IControllerFactory接口即可。
    //实际上就是替换现有的ControllerFactory，让Spring.NET容器管理Controller。
    //包含Spring.NET容器配置的Controller使用新建的ControllerFactory，
    //没有包含Spring.NET容器配置的Controller使用原有的DefaultControllerFactory。
    public class SpringControllerFacotry : IControllerFactory
    {
        private DefaultControllerFactory defaultFactory = null;
        public IController CreateController(RequestContext requestContext, string controllerName)
        {
            IApplicationContext ctx = ContextRegistry.GetContext();
            string controller = controllerName + "Controller";
            //bool isMatch = PathMatcher.Match("*Controller", controller, true);
            //检查是否在容器中注入了该Controller，注入了则使用注入的Controller
            if (ctx.ContainsObject(controller))
            {
                object currentController = ctx.GetObject(controller);
                return (IController)currentController;
            }
            else//没有配置则使用默认的factory创建Controller
            {
                if (defaultFactory == null)
                {
                    defaultFactory = new DefaultControllerFactory();
                }
                return defaultFactory.CreateController(requestContext, controllerName);
            }
        }

        public SessionStateBehavior GetControllerSessionBehavior(RequestContext requestContext, string controllerName)
        {
            return SessionStateBehavior.Default;
        }

        public void ReleaseController(IController controller)
        {
            IApplicationContext ctx = ContextRegistry.GetContext();
            if (!ctx.ContainsObject(controller.GetType().Name))
            {
                if (defaultFactory == null)
                {
                    defaultFactory = new DefaultControllerFactory();
                }
                defaultFactory.ReleaseController(controller);
            }
        }
    }
}