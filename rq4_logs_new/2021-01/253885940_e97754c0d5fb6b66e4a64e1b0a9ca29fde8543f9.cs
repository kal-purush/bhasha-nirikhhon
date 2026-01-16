using AutoVersionsDB.Core.ConfigProjects;
using AutoVersionsDB.Core.IntegrationTests;
using AutoVersionsDB.Core.IntegrationTests.DBVersionsTests;
using AutoVersionsDB.Core.IntegrationTests.DBVersionsTests.TestDefinitions.DevEnv_InitDB;
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
    public class DevEnv_InitDB_Tests
    {
        [SetUp]
        public void Init()
        {
            DIConfig.CreateKernel();
        }




        [Test]
        public void DevEnv_InitDB()
        {
            TestsRunner.RunTestsForeachDBType<DevEnv_InitDB_API, DevEnv_InitDB_CLI, DevEnv_InitDB_UI>();
        }





    }
}