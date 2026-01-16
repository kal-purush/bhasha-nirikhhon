using Microsoft.EntityFrameworkCore;
using System;
using Toolkit.DataStore;
using Xunit;
namespace Tests.DataStore
{
    public class DataContextHandlerTests
    {
        private class TestContext1 : DbContext
        {

        }

        private class TestContext2 : DbContext
        {

        }

        [Fact]
        void TestSettingAndCreatingOfContext()
        {
            // 1. CreateContext throws if not initialized
            Assert.Throws<InvalidOperationException>(() => DataContextHandler.CreateContext());

            // 2. Setting context works
            DataContextHandler.SetContext<TestContext1>();
            Assert.IsType<TestContext1>(DataContextHandler.CreateContext());

            // 3. Changing context works
            DataContextHandler.SetContext<TestContext2>();
            Assert.IsType<TestContext2>(DataContextHandler.CreateContext());
        }
    }
}