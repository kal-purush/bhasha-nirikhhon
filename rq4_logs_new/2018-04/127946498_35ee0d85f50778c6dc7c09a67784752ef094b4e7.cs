using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Feedler.Extensions
{
    public static class Timer
    {
        public struct TimedExecutionResult<T>
        {
            public T Result { get; set; }
            public TimeSpan ExecutionTime { get; set; }

            public static implicit operator T(TimedExecutionResult<T> timedResult) => timedResult.Result;

            public static implicit operator TimeSpan(TimedExecutionResult<T> timedResult) => timedResult.ExecutionTime;
        }

        public static TimeSpan Time(Action action)
        {
            var timer = new Stopwatch();
            timer.Start();

            action();

            timer.Stop();
            return timer.Elapsed;
        }

        public static TimedExecutionResult<T> Time<T>(Func<T> action)
        {
            var timer = new Stopwatch();
            timer.Start();

            var result = action();

            timer.Stop();
            return new TimedExecutionResult<T>
            {
                Result = result,
                ExecutionTime = timer.Elapsed
            };
        }

        public static async Task<TimeSpan> TimeAsync(Func<Task> action)
        {
            var timer = new Stopwatch();
            timer.Start();

            await action();

            timer.Stop();
            return timer.Elapsed;
        }

        public static async Task<TimedExecutionResult<T>> TimeAsync<T>(Func<Task<T>> action)
        {
            var timer = new Stopwatch();
            timer.Start();

            var result = await action();

            timer.Stop();
            return new TimedExecutionResult<T>
            {
                Result = result,
                ExecutionTime = timer.Elapsed
            };
        }
    }
}