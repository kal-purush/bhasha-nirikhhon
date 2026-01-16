using Autofac;
using Autofac.Extensions.DependencyInjection;
using Autofac.Features.Variance;
using Infra.Events;
using Infra.Events.Dispatching;
using Infra.IoC;
using Infra.Mixins;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Diagnostics.Contracts;
using System.Linq;

namespace Demo.MVC6
{
    public static class AutofacConfig
    {
        public static IServiceProvider Register(IServiceCollection services)
        {
            var builder = new ContainerBuilder();
            builder.RegisterSource(new ContravariantRegistrationSource());

            Types.Referenced.KindOf("Services")
                .Classes()
                .ForAll(t =>
                {
                    builder
                        .RegisterType(t)
                        .AsImplementedInterfaces()
                        .InstancePerRequest();
                });

            Types.Referenced.KindOf("Services.Singletons")
                .Classes()
                .ForAll(t =>
                {
                    builder
                        .RegisterType(t)
                        .AsImplementedInterfaces()
                        .SingleInstance();
                });

            Types.Referenced.KindOf("Services.Transient")
                .Classes()
                .ForAll(t =>
                {
                    builder
                        .RegisterType(t)
                        .AsImplementedInterfaces()
                        .InstancePerDependency();
                });

            Types.Referenced.With<MixinAttribute>()
                .ForAll(t => builder
                    .RegisterType(Mixin.Emit(t))
                    .As(t));

            builder.RegisterType<ServiceProvider>()
                .AsImplementedInterfaces();

            //Populate the container with services that were previously registered
            builder.Populate(services);

            var container = builder.Build();
            Event.Subscribe(new EventDispatcher(container.Resolve<IServiceProvider>()));
            return container.Resolve<IServiceProvider>();
        }
    }

    class ServiceProvider : IServiceProvider
    {
        public ServiceProvider(IComponentContext context)
        {
            Contract.Requires<ArgumentNullException>(context != null);
            Contract.Ensures(Context != null);
            Context = context;
        }

        public IComponentContext Context { get; }

        public object GetService(Type serviceType)
        {
            return Context.Resolve(serviceType);
        }
    }
}