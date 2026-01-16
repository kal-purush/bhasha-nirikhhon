using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cacti.Utils.AsyncUtil
{
    public class CompositeAsyncEnumerable<TIn, TOut> : IAsyncEnumerable<TOut>
    {
        private readonly IAsyncEnumerable<TIn> enumerable;
        private readonly Func<TIn, TOut> onCurrent;

        public CompositeAsyncEnumerable(IAsyncEnumerable<TIn> enumerable, Func<TIn, TOut> onCurrent)
        {
            this.enumerable = enumerable ?? throw new ArgumentNullException(nameof(enumerable));
            this.onCurrent = onCurrent ?? throw new ArgumentNullException(nameof(onCurrent));
        }

        public IAsyncEnumerator<TOut> GetAsyncEnumerator()
        {
            return new AsyncEnumerator(enumerable.GetAsyncEnumerator(), onCurrent);
        }

        private class AsyncEnumerator : IAsyncEnumerator<TOut>
        {
            public AsyncEnumerator(IAsyncEnumerator<TIn> enumerator, Func<TIn, TOut> func)
            {
                this.enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
                this.func = func ?? throw new ArgumentNullException(nameof(func));
            }

            private static Func<TOut> GetCurrentValue(TOut value)
                => () => value;

            private readonly Func<TIn, TOut> func;
            private readonly IAsyncEnumerator<TIn> enumerator;

            public TOut Current => func(enumerator.Current);

            public void Dispose()
                => enumerator.Dispose();

            public Task<bool> MoveNextAsync(CancellationToken token)
                => enumerator.MoveNextAsync(token);

            public void Reset()
                => enumerator.Reset();
        }
    }
}