using Cacti.Utils.AsyncUtil;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cacti.Utils.ObservableUtil
{
    public class AsyncEnumerableObservable<T> : IObservable<T>
    {
        private readonly IAsyncEnumerable<T> enumerable;

        public AsyncEnumerableObservable(IAsyncEnumerable<T> enumerable)
        {
            this.enumerable = enumerable ?? throw new ArgumentNullException(nameof(enumerable));
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            try
            {
                enumerable
                    .ForEach((value) => observer.OnNext(value), tokenSource.Token)
                    .ContinueWith((task) =>
                    {
                        observer.OnCompleted();
                    }, tokenSource.Token);
            }
            catch(Exception exception)
            {
                observer.OnError(exception);
            }
            
            return new Unsubscriber(tokenSource);
        }

        private class Unsubscriber : IDisposable
        {
            private readonly CancellationTokenSource tokenSource;

            public Unsubscriber(CancellationTokenSource tokenSource)
            {
                this.tokenSource = tokenSource ?? throw new ArgumentNullException(nameof(tokenSource));
            }

            public void Dispose()
            {
                tokenSource.Cancel();
            }
        }
    }
}