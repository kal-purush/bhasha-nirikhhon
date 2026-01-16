using AutoVersionsDB.Core.ConfigProjects;
using AutoVersionsDB.Core.IntegrationTests;
using AutoVersionsDB.Core.IntegrationTests.DBVersionsTests;
using AutoVersionsDB.Core.IntegrationTests.DBVersionsTests.TestDefinitions.DevEnv_VirtualDDD;
using AutoVersionsDB.Helpers;
using AutoVersionsDB.NotificationableEngine;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace AutoVersionsDB.Core.IntegrationTests.DBVersionsTests
{
    [TestFixture]
    public class DevEnv_VirtualDDD_Tests
    {
        [SetUp]
        public void Init()
        {
            DIConfig.CreateKernel();
        }




        [Test]
        public void DevEnv_VirtualDDD()
        {
            TestsRunner.RunTestsForeachDBType<DevEnv_VirtualDDD_API, DevEnv_VirtualDDD_CLI, DevEnv_VirtualDDD_UI>();
        }





    }
}