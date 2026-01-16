using System;
using Microsoft.Extensions.Configuration;
using Nancy;
using Nancy.Configuration;
using Nancy.TinyIoc;
using Polly.Registry;

namespace DistanceService
{
    public class Bootstrapper : DefaultNancyBootstrapper
    {
        private readonly IServiceProvider serviceProvider;

        public Bootstrapper(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }

        public override void Configure(INancyEnvironment environment)
        {
            environment.Tracing(true, true);
        }

        protected override void ConfigureApplicationContainer(TinyIoCContainer container)
        {
            base.ConfigureApplicationContainer(container);
            container.Register((IConfiguration)serviceProvider.GetService(typeof(IConfiguration)));
            container.Register((IReadOnlyPolicyRegistry<string>)serviceProvider.GetService(typeof(IReadOnlyPolicyRegistry<string>)));
        }
    }
}