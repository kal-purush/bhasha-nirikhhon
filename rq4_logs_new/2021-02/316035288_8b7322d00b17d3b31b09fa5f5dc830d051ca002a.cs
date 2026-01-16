using System;
using System.Threading.Tasks;

namespace Chatbot.Abstractions.Pipe
{
    public class PipeHandler
    {
        public virtual Task InvokeAsync(object context, Func<Task> next)
        {
            return Task.CompletedTask;
        }
    }

    public abstract class PipeHandler<TPipeContext>: PipeHandler where TPipeContext: IPipeContext
    {
        protected abstract Task InvokeAsync(TPipeContext context, Func<Task> next);

        public sealed override Task InvokeAsync(object context, Func<Task> next)
        {
            return InvokeAsync((TPipeContext) context, next);
        }
    }
}