using System;
using System.Threading.Tasks;

using Browser.xUnit;

using Xunit;
using Xunit.Abstractions;

namespace ExampleTest
{
    public class ExampleTest : INeedToKnowTestFailure
    {

        [BrowserFact]
        public void PassingTest()
        {
            
        }

        [BrowserFact]
        public void FailingTest()
        {
            Assert.True(false, "Failing Test");
        }

        #region Implementation of INeedToKnowTestFailure

        public Task HandleFailureAsync(ITest test, Exception exception)
        {
            Console.WriteLine("Handling test failure!");
            return Task.FromResult(true);
        }

        #endregion
    }
}