using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Bot.Builder;

namespace HackedBrain.BotBuilder.State.Conditional
{
    internal sealed class ConditionalStatePropertyAccessor<T> : IStatePropertyAccessor<T>
    {
        public ConditionalStatePropertyAccessor()
        {
        }

        public IStatePropertyAccessor<T> DefaultAccessor { get; set; }

        public List<(Func<ITurnContext, bool> Selector, IStatePropertyAccessor<T> StatePropertyAccessor)> ConditionalAccessors { get; } = new List<(Func<ITurnContext, bool> Selector, IStatePropertyAccessor<T> StatePropertyAccessor)>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IStatePropertyAccessor<T> SelectAccessorForTurn(ITurnContext turnContext)
        {
            var selectedAccessor = ConditionalAccessors.FirstOrDefault(ca => ca.Selector(turnContext)).StatePropertyAccessor ?? DefaultAccessor;

            if (selectedAccessor == null)
            {
                throw new Exception($"No conditional accessor was selected for the given turn context and no default accessor is registered.");
            }

            return selectedAccessor;
        }

        public string Name => nameof(ConditionalStatePropertyAccessor<T>);

        public Task DeleteAsync(ITurnContext turnContext, CancellationToken cancellationToken = default(CancellationToken)) =>
            SelectAccessorForTurn(turnContext).DeleteAsync(turnContext, cancellationToken);

        public Task<T> GetAsync(ITurnContext turnContext, Func<T> defaultValueFactory = null, CancellationToken cancellationToken = default(CancellationToken)) =>
            SelectAccessorForTurn(turnContext).GetAsync(turnContext, defaultValueFactory, cancellationToken);

        public Task SetAsync(ITurnContext turnContext, T value, CancellationToken cancellationToken = default(CancellationToken)) =>
            SelectAccessorForTurn(turnContext).SetAsync(turnContext, value, cancellationToken);
    }
}