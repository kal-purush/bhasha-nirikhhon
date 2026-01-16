
// ReSharper disable once CheckNamespace
using Rocket.Surgery.Extensions.Serilog;

namespace Rocket.Surgery.Conventions
{
    /// <summary>
    /// Helper method for working with <see cref="IConventionHostBuilder" />
    /// </summary>
    public static class SerilogAbstractionsHostBuilderExtensions
    {
        /// <summary>
        /// Configure the serilog delegate to the convention scanner
        /// </summary>
        /// <param name="container">The container.</param>
        /// <param name="delegate">The delegate.</param>
        /// <returns>IConventionHostBuilder.</returns>
        public static IConventionHostBuilder ConfigureSerilog(
            this IConventionHostBuilder container,
            SerilogConventionDelegate @delegate)
        {
            container.Scanner.AppendDelegate(@delegate);
            return container;
        }
    }
}