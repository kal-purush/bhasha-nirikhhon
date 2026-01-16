using Xunit;

using static Chakra.Test.TestHelper;
namespace Chakra.Test
{
    public class ProgramTest
    {
        [Fact]
        public void ShouldRunAProgram()
        {
            string snippet =  @"
            using System;
            namespace Client
            {
                class Program
                {
                    static void Main(string[] args)
                    {
                      Console.WriteLine(""Hello"");
                        Console.WriteLine(""World !"");
                    }
                }
            }";

            string expected = "Hello World !";

            ExpectOutput(snippet, "Hello", "World !");
        }
     
        
        private void ExpectOutput(string snippet, params string[] expected)
        {
            Assert.Equal(LinesOf(expected), Executor.Execute(snippet));
        }
    }
}