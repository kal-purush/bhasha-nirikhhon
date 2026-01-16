
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

[Generator]
public class DbContextGenerator : ISourceGenerator
{
    public void Execute(GeneratorExecutionContext context)
    {
        var syntaxTrees = context.Compilation.SyntaxTrees;

        foreach (var syntaxTree in syntaxTrees)
        {
            var dbModelTypes = syntaxTree.GetRoot().DescendantNodes().OfType<ClassDeclarationSyntax>()
                .Where(x => x.AttributeLists.Any(y => y.ToString().StartsWith("[DbModel")))
                .ToList();

string source = $@"
using Microsoft.EntityFrameworkCore;

public class MyDbContext : DbContext{{
    public MyDbContext(DbContextOptions options) : base(options){{

    }}
";
            var 
            foreach (var model in dbModelTypes)
            {
                source += $@"public DbSet<{model.Identifier}> {model.Identifier} {{get;set;}}" + '\n';
            }

            source += "}}";

            context.AddSource("MyDbContext.g.cs", source);
        }
    }

    public void Initialize(GeneratorInitializationContext context)
    {

    }
}