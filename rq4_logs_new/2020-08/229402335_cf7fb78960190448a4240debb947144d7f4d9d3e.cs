using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace Sample.ConsoleApp.Tests
{
    [TestClass]
    public class ProgramTest
    {
        [TestMethod]
        public void Given_Arguments_When_Main_Invoked_Then_It_Should_Return_Result()
        {
            var args = new[] { "dotnet", "core" }.ToArray();

            Program.Main(args);

            Program.Model.Arguments.First().Should().Be("dotnet");
        }
    }
}