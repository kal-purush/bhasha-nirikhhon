using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Xunit;

namespace Messerli.Test.Utility.Test
{
    public class TestEnvironmentBuilderTest 
    {
        [Fact]
        public void CreateTestEnvironmentBuilder()
        {
            var testEnvironmentBuilder = new TestEnvironmentBuilder();
            
            var testFile = new TestFile("file1.txt");
            testEnvironmentBuilder.AddTestFile(testFile);
            var testEnvironmentProvider = testEnvironmentBuilder.Build();

            var tempPath = Path.GetTempPath();
            Assert.True(File.Exists(Path.Combine(tempPath, testEnvironmentProvider.RootDirectory, testFile.RelativeFilePath)));

        }
    }
}