using Cacti.Utils.ObservableUtil;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cacti.Utils.JobUtil
{
    public static class JobObservable
    {
        public static (IJob job, IObservable<T> observable) Get<T>(Func<T> onExecute)
            => Get(token => Task.FromResult(onExecute()));

        public static (IJob job, IObservable<T> observable) Get<T>(Func<Task<T>> onExecute)
            => Get(token => onExecute());

        public static (IJob job, IObservable<T> observable) Get<T>(Func<CancellationToken, Task<T>> onExecute)
        {
            Observable<T> observable = new Observable<T>();
            IJob job = new JobObservable<T>(onExecute, observable);
            return (job, observable);
        }
    }

    public class JobObservable<T> : IJob
    {
        private readonly Func<CancellationToken, Task<T>> onExecute;
        private readonly Observable<T> observable;

        public JobObservable(Func<CancellationToken, Task<T>> onExecute, Observable<T> observable)
        {
            this.onExecute = onExecute ?? throw new ArgumentNullException(nameof(onExecute));
            this.observable = observable ?? throw new ArgumentNullException(nameof(observable));
        }

        public async Task Execute(CancellationToken token)
        {
            try
            {
                T value = await onExecute(token);
                observable.Next(value);
            }
            catch(Exception exception)
            {
                observable.Error(exception);
            }
        }

        public void Dispose()
        {
            observable.Complete();
        }
    }
}