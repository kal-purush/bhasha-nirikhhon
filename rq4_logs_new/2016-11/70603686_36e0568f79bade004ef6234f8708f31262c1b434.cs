using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Clivis.Models.Nibe;
using System.IO;

namespace UnitTestsClivis
{
    public class NibeUnitTests
    {
        private NibeUnit nibeUnit;
        private string codeFilePath = "code.txt";

        public NibeUnitTests()
        {
            nibeUnit = new NibeUnit() { clientId = "12345", code = null, passWord = "qwert", redirect_uri = "http://somuri", secret = "mysecret", userName = "myusername@user.se" };
            nibeUnit.CodeFilePath = codeFilePath;
        }
        [Fact]
        public void NibeUnit_code_setter_CodeIsWrittenToFile()
            {
            if (File.Exists(codeFilePath))
                File.Delete(codeFilePath);

            Assert.False(File.Exists(codeFilePath), "The code file could not be deleted");
            
            nibeUnit.code = "newcode";
            Assert.True(File.Exists(codeFilePath));
            string fileContent = File.ReadAllText(codeFilePath);

            // Assert the file contents
            Assert.Equal("newcode", fileContent);

            // Assert that the variable is set in unit
            Assert.Equal("newcode", nibeUnit.code);

            // Cleanup
            File.Delete(codeFilePath);

            Assert.False(File.Exists(codeFilePath), "The code file could not be deleted " + new FileInfo(codeFilePath).FullName);

        }

    }
}