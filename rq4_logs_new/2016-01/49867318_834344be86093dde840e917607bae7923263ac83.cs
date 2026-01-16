using System.Web.Http;
using RefactoringApplication.Api.Domain.Services;
using RefactoringApplication.Api.Infrastructure;
using SimpleInjector;
using SimpleInjector.Integration.WebApi;

namespace RefactoringApplication.Api
{
    public static class SimpleInjectorConfig
    {
        public static void ConfigureContainer()
        {
            var container = new Container();
            container.Options.DefaultScopedLifestyle = new WebApiRequestLifestyle();

            PerformRegistrations(container);

            container.RegisterWebApiControllers(GlobalConfiguration.Configuration);

            container.Verify();

            GlobalConfiguration.Configuration.DependencyResolver =
                new SimpleInjectorWebApiDependencyResolver(container);

        }

        private static void PerformRegistrations(Container container)
        {
            container.Register<ICustomLogger, ConsoleLogger>(Lifestyle.Scoped);
            container.Register<ICompanyLoader>(() =>
            {
                if (CurrentConfiguration.LoadFromJson)
                {
                    return new JsonCompanyLoader(container.GetInstance<ICustomLogger>());
                }
                else
                {
                    return new HardCodedCompanyLoader(container.GetInstance<ICustomLogger>());
                }
            });
            container.RegisterDecorator(typeof (ICompanyLoader), typeof (CompanyLoaderLoggingDecorator));
        }
    }
}