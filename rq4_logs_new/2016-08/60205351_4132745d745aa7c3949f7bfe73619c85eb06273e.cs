using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using zavit.Domain.ExternalAccounts;
using zavit.Infrastructure.ExternalAccounts.Repositories;

namespace zavit.Infrastructure.Ioc.Infrastructure
{
    public class InfrastructureExternalAccounts : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(
                Component.For<IExternalAccountsRepository>().ImplementedBy<ExternalAccountsRepository>().LifestyleTransient());
        }
    }
}