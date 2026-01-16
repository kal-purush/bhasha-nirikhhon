using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cacti.Utils.UnitTests
{
    [TestClass]
    public class UnitTest
    {
        [TestMethod]
        public async Task DoJob()
        {
            bool done = false;
            IJob job = new Job(() => { done = true; });

            await job.Execute(CancellationToken.None);

            Assert.AreEqual(done, true);
        }

        [TestMethod]
        public async Task ThenJob()
        {
            bool done = false;
            IJob job = new Job(() => { })
                .Then(new Job(() => { done = true; }));

            await job.Execute(CancellationToken.None);

            Assert.AreEqual(done, true);
        }

        [TestMethod]
        public async Task CancelJob()
        {
            bool done = false;
            IJob job = new Job(() => { })
                .Delay(TimeSpan.FromMilliseconds(100))
                .Then(new Job(() => { done = true; }));

            using (job.Run())
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50));
            }

            Assert.AreNotEqual(done, true);
        }

        [TestMethod]
        public async Task RepeatJob()
        {
            int repeatTimes = 0;
            IJob job = new Job(() => repeatTimes++)
                .Repeat(TimeSpan.FromMilliseconds(10));

            using (job.Run())
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50));
            }

            Assert.AreEqual(repeatTimes, 5);
        }

        [TestMethod]
        public async Task TimesJob()
        {
            int repeatTimes = 0;
            IJob job = new Job(() => repeatTimes++)
                .Times(5);

            await job.Execute(CancellationToken.None);

            Assert.AreEqual(repeatTimes, 5);
        }

        [TestMethod]
        public async Task AggregateJob()
        {
            bool job1Done = false;
            bool job2Done = false;

            await new[]
            {
                new Job(() => job1Done = true),
                new Job(() => job2Done = true)
            }
            .Aggregate()
            .Execute(CancellationToken.None);

            Assert.AreEqual(job1Done, true);
            Assert.AreEqual(job2Done, true);
        }

        [TestMethod]
        public async Task RunAndRetry()
        {
            int retryTimes = 5;
            IJob job = new Job(() => throw new Exception());
            IJob retryAwaiter = new Job(() => { })
                .Repeat(TimeSpan.Zero, () => retryTimes > 0);

            using (job.Run(exception =>
            {
                retryTimes--;
                return retryTimes > 0;
            }))
            {
                await retryAwaiter.Execute(CancellationToken.None);
            }

            Assert.AreEqual(retryTimes, 0);
        }
    }
}