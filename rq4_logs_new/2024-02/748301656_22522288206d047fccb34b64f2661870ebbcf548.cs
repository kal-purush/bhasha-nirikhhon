namespace SharpAttributeParser;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

public static class CompilationStore
{
    private static Compilation? EmptyCompilation;

    private static readonly CSharpParseOptions ParseOptions = new(languageVersion: LanguageVersion.CSharp11);
    private static readonly CSharpCompilationOptions CompilationOptions = new(OutputKind.DynamicallyLinkedLibrary);

    public static Compilation GetCompilation(string source)
    {
        var emptyCompilation = EmptyCompilation ??= CreateEmptyCompilation();

        var syntaxTree = CSharpSyntaxTree.ParseText(source, ParseOptions);

        return emptyCompilation.AddSyntaxTrees(syntaxTree);
    }

    public static async Task<(Compilation, AttributeData, AttributeSyntax)> GetComponents(string source, string typeName)
    {
        var compilation = GetCompilation(source);

        var type = compilation.GetTypeByMetadataName(typeName)!;

        var attributeData = type.GetAttributes()[0];

        var syntax = (AttributeSyntax)await attributeData.ApplicationSyntaxReference!.GetSyntaxAsync();

        return (compilation, attributeData, syntax);
    }

    private static CSharpCompilation CreateEmptyCompilation()
    {
        var references = ListAssemblies()
            .Where(static (assembly) => assembly.IsDynamic is false)
            .Select(static (assembly) => MetadataReference.CreateFromFile(assembly.Location))
            .Cast<MetadataReference>();

        return CSharpCompilation.Create("FakeAssembly", references: references, options: CompilationOptions);
    }

    public static IEnumerable<Assembly> ListAssemblies()
    {
        Queue<Assembly> unresolvedAssemblies = new();
        List<Assembly> resolvedAssemblies = [];

        HashSet<string> resolvedAssemblyNames = [];

        unresolvedAssemblies.Enqueue(Assembly.GetExecutingAssembly());

        while (unresolvedAssemblies.Count != 0)
        {
            var targetAssembly = unresolvedAssemblies.Dequeue();

            foreach (var assemblyName in targetAssembly.GetReferencedAssemblies().Where((assemblyName) => resolvedAssemblyNames.Contains(assemblyName.FullName) is false))
            {
                resolvedAssemblyNames.Add(assemblyName.FullName);

                Assembly assemblyReference;

                try
                {
                    assemblyReference = Assembly.Load(assemblyName);
                }
                catch (Exception e) when (e is FileLoadException or FileNotFoundException)
                {
                    continue;
                }

                unresolvedAssemblies.Enqueue(assemblyReference);
                resolvedAssemblies.Add(assemblyReference);
            }
        }

        resolvedAssemblies.AddRange(AppDomain.CurrentDomain.GetAssemblies());

        return resolvedAssemblies;
    }
}