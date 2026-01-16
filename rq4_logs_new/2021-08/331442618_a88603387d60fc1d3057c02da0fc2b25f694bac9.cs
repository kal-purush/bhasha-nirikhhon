using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FakeItEasy;
using NUnit.Framework;
using Stackage.Core.Abstractions.Metrics;
using Stackage.Core.Polly;
using Stackage.Core.RateLimiting;

namespace Stackage.Core.Tests.Polly.RateLimit
{
   public class triple_execute_one_request_per_period
   {
      private int _executeCallCount;
      private long _durationMs;

      [OneTimeSetUp]
      public async Task setup_scenario()
      {
         var rateLimiter = new RateLimiter(1, TimeSpan.FromMilliseconds(100), 1, TimeSpan.FromMinutes(1));
         var policyFactory = new PolicyFactory(A.Fake<IMetricSink>(), A.Fake<ITimerFactory>());
         var rateLimitPolicy = policyFactory.CreateAsyncRateLimitingPolicy(rateLimiter);

         var stopwatch = Stopwatch.StartNew();

         for (var i = 0; i < 3; i++)
         {
            await rateLimitPolicy.ExecuteAsync(() =>
            {
               _executeCallCount++;

               return Task.CompletedTask;
            });
         }

         _durationMs = stopwatch.ElapsedMilliseconds;
      }

      [Test]
      public void should_have_executed_three_times()
      {
         Assert.That(_executeCallCount, Is.EqualTo(3));
      }

      [Test]
      public void should_wait_for_limit_period_for_second_execute()
      {
         Assert.That(_durationMs, Is.InRange(160, 240));
      }
   }
}