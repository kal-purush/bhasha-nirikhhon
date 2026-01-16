using Cacti.Utils.StepUtil;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cacti.Utils.UnitTests
{
    [TestClass]
    public class StepUnitTest
    {
        [TestMethod]
        public async Task SingleStep()
        {
            const string VALUE = "test";

            IStep<string, string> step = new Step<string, string>((string text) => text);
            string result = await step.Execute(VALUE, CancellationToken.None);

            Assert.AreEqual(result, VALUE);
        }

        [TestMethod]
        public async Task ChainStep()
        {
            IStep<int, string> step = new Step<int, int>((int count) => ++count)
                .Then(new Step<int, string>((int count) => count.ToString()));

            string result = await step.Execute(0, CancellationToken.None);

            Assert.AreEqual(result, "1");
        }

        [TestMethod]
        public async Task RoutingStep()
        {
            IStep<int, string> stepWhen1 = new Step<int, string>((count) => $"count : {count}");
            IStep<int, string> stepWhen2 = new Step<int, string>((count) => $"count : {++count}");
            IStep<int, int> stepCount = new Step<int, int>((int count) => ++count);

            IStep<int, string> step = stepCount
                .Then((count) => count == 1 
                    ? stepWhen1
                    : stepWhen2);

            string result = await step.Execute(0, CancellationToken.None);

            Assert.AreEqual(result, "count : 1");
        }
    }
}