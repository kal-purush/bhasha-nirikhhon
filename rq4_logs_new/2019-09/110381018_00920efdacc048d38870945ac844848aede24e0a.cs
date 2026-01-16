using Serilog;
using Serilog.Configuration;
using Rocket.Surgery.Conventions;
using Rocket.Surgery.Extensions.Serilog.Conventions;

namespace Rocket.Surgery.Extensions.Serilog.Conventions
{
    /// <summary>
    ///  SerilogConditionallyAsyncLoggingConvention.
    /// Implements the <see cref="ISerilogConvention" />
    /// </summary>
    /// <seealso cref="ISerilogConvention" />
    public abstract class SerilogConditionallyAsyncLoggingConvention : ISerilogConvention
    {
        /// <summary>
        /// Registers the specified context.
        /// </summary>
        /// <param name="context">The context.</param>
        public void Register(ISerilogConventionContext context)
        {
            context.WriteToAsyncConditionally(Register);
        }

        /// <summary>
        /// Registers the sink synchronously or asynchronously
        /// </summary>
        /// <param name="configuration">The sink configuration.</param>
        protected abstract void Register(LoggerSinkConfiguration configuration);
    }
}