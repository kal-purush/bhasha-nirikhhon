using System;
using System.Collections.Generic;
using System.Web.Http;
using System.Web.Http.Dependencies;
using System.Web.Mvc;
using MVCApp.Services;
using Microsoft.Practices.Unity;
using MVCApp.Interfaces;
using Unity.Mvc5;
using Unity;

namespace DemoMVC
{
    public sealed class UnityConfig
    {
        private readonly static UnityConfig _instance = new UnityConfig();

        static UnityConfig() { }

        private UnityConfig() { }

        public static UnityConfig Instance
        {
            get
            {
                return _instance;
            }
        }

        public void RegisterComponents(HttpConfiguration config)
        {
            var container = new UnityContainer();

            // register all your components with the container here
            // it is NOT necessary to register your controllers

            // e.g. container.RegisterType<ITestService, TestService>();
            container.RegisterType<ITrainerService, TrainerService>();

            DependencyResolver.SetResolver(new UnityDependencyResolver(container));
            config.DependencyResolver = new UnityResolver(container);
        }
    }

    public class UnityResolver : System.Web.Http.Dependencies.IDependencyResolver
    {
        protected IUnityContainer container;

        public UnityResolver(IUnityContainer container)
        {
            if (container == null)
                throw new ArgumentException("Missing container");

            this.container = container;
        }

        public object GetService(Type serviceType)
        {
            try
            {
                return container.Resolve(serviceType);
            }
            catch (ResolutionFailedException)
            {
                return null;
            }
        }

        public IEnumerable<object> GetServices(Type serviceType)
        {
            try
            {
                return container.ResolveAll(serviceType);
            }
            catch (ResolutionFailedException)
            {
                return new List<object>();
            }
        }

        public IDependencyScope BeginScope()
        {
            IUnityContainer child = container.CreateChildContainer();
            return new UnityResolver(child);
        }

        public void Dispose()
        {
            container.Dispose();
        }
    }
}