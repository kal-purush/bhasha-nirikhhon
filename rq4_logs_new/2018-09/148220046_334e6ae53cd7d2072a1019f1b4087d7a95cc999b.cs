using Amdocs.Ginger.Plugin.Core;
using GingerShellPlugin;
using GingerTestHelper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace GingerShellPluginTest
{
    [TestClass]
    public class DirServiceUnitTests
    {
            private static string testFolderName = "DirServiceTests";

            [ClassInitialize]
            public static void ClassInitialize(TestContext TestContext)
            {
                EmptyTempFolder();
            }

            [ClassCleanup]
            public static void ClassCleanup()
            {
                //
            }

            [TestInitialize]
            public void TestInitialize()
            {
                // before every test
            }

            [TestCleanup]
            public void TestCleanUp()
            {
                //after every test
            }


            [TestMethod]
        public void TestGingerDirService_CheckDirExists()
        {
            //Arrange
            string tempFolder = TestResources.GetTempFile("") + "\\" + testFolderName;

            DirService dirService = new DirService();
            GingerAction gingerAct = new GingerAction();

            //Act
            dirService.DirExists(gingerAct, tempFolder);

            //Assert
            Assert.AreEqual(gingerAct.GetOutputValue("DirExists"), "True");
        }

        [TestMethod]
        public void TestGingerDirService_RunDirInfoCommand()
        {
            //Arrange
            string tempFolder = TestResources.GetTempFile("") + "\\" + testFolderName;

            DirService dirService = new DirService();
            GingerAction gingerAct = new GingerAction();

            //Act
            dirService.DirInfo(gingerAct, tempFolder);

            //Assert
            Assert.IsNotNull(gingerAct.GetOutputValue("DirInfo_Root"));
            Assert.AreEqual(gingerAct.GetOutputValue("DirInfo_Name"), testFolderName);
        }


        private static void EmptyTempFolder()
        {
            string tempFolder = TestResources.GetTempFile("") + "\\" + testFolderName;
            if (System.IO.Directory.Exists(tempFolder))
            {
                System.IO.DirectoryInfo directory = new DirectoryInfo(tempFolder);
                foreach (System.IO.FileInfo file in directory.GetFiles())
                {
                    file.Delete();
                }
            }
            else
            {
                System.IO.Directory.CreateDirectory(tempFolder);
            }
        }

    }
}