using System;
using System.IO;
using Xunit;

namespace Exercise01.Tests
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            //Assign
            var writer = new StringWriter();
            Console.SetOut(writer);

            //Act
            Program.Main(new string[0]);

            //Assert
            var output = writer.GetStringBuilder().ToString().Trim();
            Assert.Equal(output,"Hello World!");
        }
    }
}