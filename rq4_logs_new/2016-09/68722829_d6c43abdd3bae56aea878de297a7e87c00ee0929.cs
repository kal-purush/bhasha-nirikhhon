using System;
using System.IO;
using NUnit.Framework;

namespace Wonga.ServiceTesting.EndpointLauncher.Tests
{
    [SetUpFixture]
    public class WithinCurrentDirectory
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            Directory.SetCurrentDirectory(AppDomain.CurrentDomain.BaseDirectory);
        }
    }
}