using Microsoft.CodeAnalysis;
using Xunit;

using static Chakra.Test.TestHelper;
namespace Chakra.Test
{
    public class AssemblyTest
    {

        [Fact]
        public void ShouldGiveCompileErrorIfAssemblyMissing()
        {
            Executor executor = new Executor(new SnippetProgramGenerator());
            var customImports = new string[] {"using System.ComponentModel;"};

            string snippet = @"
                EventDescriptor a = null;
                Console.WriteLine(""not error"");
            ";

            Assert.Throws<DynamicCompilationException>(() => 
                            executor.ExecuteSnippet(BreakLines(snippet), customImports
            ));
        }
        
        [Fact]
        public void ShouldAllowAddingCustomAssemblyAndImports()
        {
            var custom = new MetadataReference[] {
                MetadataReference.CreateFromFile(typeof(System.ComponentModel.Container).Assembly.Location),
            };
            var customImports = new string[] {"using System.ComponentModel;"};
            
            Executor executor = new Executor(new SnippetProgramGenerator(), custom);

            string snippet = @"
                EventDescriptor a = null;
                Console.WriteLine(""not error"");
            ";

            var expected = "not error";
            var actual = executor.ExecuteSnippet(BreakLines(snippet), customImports);

            Assert.Equal(expected, actual);
        }
    }
}