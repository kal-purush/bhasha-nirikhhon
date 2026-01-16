using System.Collections.Generic;
using System.Linq;
using Stackage.Core.Abstractions.Metrics;

namespace Stackage.Core.Tests
{
   public class StubTimerFactory : ITimerFactory
   {
      private readonly Queue<Timer> _timers;

      public StubTimerFactory(params long[] elapsedMillisecondsList)
      {
         _timers = new Queue<Timer>(elapsedMillisecondsList.Select(c => new Timer(c)));
      }

      public ITimer CreateAndStart()
      {
         return _timers.Dequeue();
      }

      private class Timer : ITimer
      {
         public Timer(long elapsedMilliseconds)
         {
            ElapsedMilliseconds = elapsedMilliseconds;
         }

         public long ElapsedMilliseconds { get; }

         public void Stop()
         {
         }
      }
   }
}