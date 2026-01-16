namespace LagDaemon.YAMUD.Scripting.Tests
{
    public class PythonScriptingContainerTests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void ScriptingContainer_Executes_Python_Code()
        {
            var container = ScriptingContainer.CreatePythonContainer();
            container.Execute("a = 3 + 4");
            var result = container.GetVariable("a");
            Assert.AreEqual(7, result);
        }

        [Test]
        public void ScriptingContainer_Executes_Python_Code_which_calls_BackToAssembly()
        {
            var python = @"
import clr
import sys
clr.AddReference('LagDaemon.YAMUD.Scripting.Tests')  
from LagDaemon.YAMUD.Scripting.Tests import InteropTest
interop_instance = InteropTest()
result = interop_instance.Add(3, 4)
";

            var container = ScriptingContainer.CreatePythonContainer();
            container.Execute(python);
            var result = container.GetVariable("result");
            Assert.AreEqual(7, result);
        }

    }
}