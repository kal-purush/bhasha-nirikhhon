using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CSharp.RuntimeBinder;
using System.Reflection;
using System.Text;

namespace Roslyn.Issue
{
    public class Program
    {
        private static readonly MetadataReference[] References =
        {
            MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(RuntimeBinderException).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(System.Runtime.CompilerServices.DynamicAttribute).GetTypeInfo().Assembly.Location)
        };

        public static void Main(string[] args)
        {
            var code = SyntaxFactory.ParseSyntaxTree(@"
namespace Roslyn.Issue
{ 
    public class Issue
    { 
        public void Operations()
        { 
            dynamic a = 5; 
            dynamic b = 1; 
            dynamic c = a + b; 
        } 
    } 
}");

            var compilation = CSharpCompilation.Create(
                assemblyName: new Guid() + ".dll",
                syntaxTrees: new[] { code },
                references: References,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(OptimizationLevel.Release)
                );

            var stream = new MemoryStream();
            var result = compilation.Emit(stream);

            if (result.Success == false)
            {
                if (result.Success == false)
                {
                    IEnumerable<Diagnostic> failures = result.Diagnostics.Where(diagnostic =>
                     diagnostic.IsWarningAsError ||
                     diagnostic.Severity == DiagnosticSeverity.Error);

                    var sb = new StringBuilder();
                    sb.AppendLine();
                    sb.AppendLine(code.ToString());
                    sb.AppendLine();

                    foreach (var diagnostic in failures)
                        sb.AppendLine(diagnostic.ToString());

                    throw new InvalidOperationException(sb.ToString());
                }
            }

            Console.WriteLine("Compiled. Press any key to exit...");
            Console.ReadLine();
        }
    }
}