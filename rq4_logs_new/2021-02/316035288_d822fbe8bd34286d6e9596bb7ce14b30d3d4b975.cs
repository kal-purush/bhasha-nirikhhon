using System;
using Chatbot.Abstractions.Pipe;
using Chatbot.Core.Pipe;
using Microsoft.Extensions.DependencyInjection;

namespace Chatbot.Hosting.Extensions
{
    public static class PipeConfigurationExtension
    {
        public static void AddPipe<TPipe, TImplementation>(this IServiceCollection services,
            Func<PipeConfigurator, PipeConfigurator> options) 
            where TPipe: class
            where TImplementation: class, TPipe
        {
            services.AddTransient(typeof(TPipe), provider =>
            {
                var configurator = new PipeConfigurator();
                foreach (var service in options(configurator).GetHandlerServices())
                {
                    configurator.AddHandler((PipeHandler) provider.GetService(service));
                }

                return Activator.CreateInstance(typeof(TImplementation), configurator);
            });
            
        }
    }
}