using System;
using Xunit;

namespace TestEnvironmentSetup.Tests
{
    public class UnitTest1
    {
        [Fact]
        public void FailingTest()
        {
            Assert.True(false);
        }

        [Fact]
        public void PassingTest()
        {
            Assert.True(true);
        }
    }
}