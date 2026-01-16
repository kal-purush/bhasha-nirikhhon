using Fidget.Commander;
using Fidget.Commander.Dispatch;
using System;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>
    /// Extension methods for registering Fidget.Commander with the stock dependency injection.
    /// </summary>

    public static class FidgetCommanderRegistration
    {
        /// <summary>
        /// Adds Fidget.Commander to the services collection.
        /// You will need to register your handlers and decorators independently.
        /// </summary>
        /// <param name="services">Services collection.</param>
        /// <remarks>
        /// When using Fidget.Commander with the standard dependency injection container, it is recommended
        /// that you also use Scrutor to add all your handlers and decorators by scanning the assembly.
        /// </remarks>
        
        public static IServiceCollection AddFidgetCommander( this IServiceCollection services )
        {
            if ( services == null ) throw new ArgumentNullException( nameof( services ) );

            services.AddTransient( typeof(ICommandAdapter<,>), typeof(CommandAdapter<,>) );
            services.AddTransient<ICommandAdapterFactory,CommandAdapterFactory>();
            services.AddTransient<ICommandDispatcher,CommandDispatcher>();

            return services;
        }
    }
}