using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

[Generator]
public class RepoSourceGenerator : ISourceGenerator
{
    public void Execute(GeneratorExecutionContext context)
    {
        GenDbContext(context);

        GenRepository(context);

        GenRepoWrapper(context);
    }

    private void GenDbContext(GeneratorExecutionContext context)
    {
        var helper = new SyntaxHelper();
        var syntaxTrees = context.Compilation.SyntaxTrees;

        string source = string.Empty;
        var mainNamespace = context.Compilation.GetEntryPoint(context.CancellationToken).ContainingNamespace.ToDisplayString();

        foreach (var syntaxTree in syntaxTrees)
        {
            var dbModelTypes = syntaxTree.GetRoot().DescendantNodes().OfType<ClassDeclarationSyntax>()
                .Where(x => x.AttributeLists.Any(y => y.ToString().StartsWith("[DbModel")))
                .ToList();

            if (dbModelTypes.Count == 0)
                continue;

            source = $@"
using Microsoft.EntityFrameworkCore;

namespace @namespace{{
    public partial class GenerateDbContext : DbContext{{
        public GenerateDbContext(DbContextOptions options) : base(options){{

        }}
";
            var listModelNamespaces = new List<string>();
            var currentNamespace = string.Empty;
            foreach (var model in dbModelTypes)
            {
                source += $@"public DbSet<{model.Identifier}> {model.Identifier} {{get;set;}}" + '\n';

                var namespaceSyntaxFound = helper.GetParentSyntax<NamespaceDeclarationSyntax>(model, out var namespaceDeclaration);

                if (namespaceSyntaxFound && namespaceDeclaration.Name.ToString() != currentNamespace)
                {
                    currentNamespace = namespaceDeclaration.Name.ToString();
                    listModelNamespaces.Add(currentNamespace);
                }
            }

            string namespaceStr = string.Empty;

            foreach (var ns in listModelNamespaces)
            {
                namespaceStr += $"using {ns}; \n";
            }

            source = namespaceStr + source + '\n';

            source += "}}";

            if (listModelNamespaces.Count > 0)
            {
                // Get first namespace
                var firstNamespace = listModelNamespaces.First();

                var projectNamespace = firstNamespace.Split('.')[0];

                source = source.Replace("@namespace", projectNamespace);
            }

            if (!string.IsNullOrEmpty(source))
                context.AddSource("GenerateDbContext.g.cs", source);
        }
    }

    private void GenRepository(GeneratorExecutionContext context)
    {
        var syntaxHelper = new SyntaxHelper();
        var syntaxTrees = context.Compilation.SyntaxTrees;

        var mainNamespace = context.Compilation.GetEntryPoint(context.CancellationToken).ContainingNamespace.ToDisplayString();

        foreach (var syntaxTree in syntaxTrees)
        {
            var types = syntaxTree.GetRoot().DescendantNodes().OfType<ClassDeclarationSyntax>()
                .Where(x => x.AttributeLists.Any(y => y.ToString().StartsWith("[DbModel")))
                .ToList();

            foreach (var model in types)
            {
                syntaxHelper.GetParentSyntax<NamespaceDeclarationSyntax>(model, out var namespaceSyntax);

                var source = $@"
using System;
using Repository.Implements;
using {namespaceSyntax.Name};
using Microsoft.EntityFrameworkCore;

namespace {namespaceSyntax.Name.ToString().Split('.')[0]}
{{
    public class {model.Identifier}Repository : Repository<{model.Identifier}> {{
        public {model.Identifier}Repository(DbContext context) : base(context){{
            /////////
        }}
    }}
}}
";
                context.AddSource($"{model.Identifier}Repo.g.cs", source);
            }
        }
    }

    private void GenRepoWrapper(GeneratorExecutionContext context) {
        var syntaxTrees = context.Compilation.SyntaxTrees;

        var helper = new SyntaxHelper();

        string source = string.Empty;
        foreach (var syntax in syntaxTrees)
        {
            var modelTypes = syntax.GetRoot().DescendantNodes().OfType<ClassDeclarationSyntax>()
                .Where(x => x.AttributeLists.Any(y => y.ToString().StartsWith("[DbModel")))
                .ToList();

            if(modelTypes.Count == 0)
                continue;

            source = $@"
using Microsoft.EntityFrameworkCore;
using @namespace;

namespace @namespace{{
    public class RepositoryWrapper : IRepositoryWrapper<GenerateDbContext>{{
    public GenerateDbContext dbContext {{get; set;}}

    public RepositoryWrapper(GenerateDbContext context)
    {{
        dbContext = context;
    }}
";

            var listModelNamespaces = new List<string>();
            var currentNamespace = string.Empty;
            foreach (var model in modelTypes)
            {
                source += $@"public {model.Identifier}Repository _{model.Identifier} {{
                    get {{
                        return new {model.Identifier}Repository(dbContext);
                    }}
                }}" + '\n';

                var namespaceSyntaxFound = helper.GetParentSyntax<NamespaceDeclarationSyntax>(model, out var namespaceDeclaration);

                if(namespaceSyntaxFound && namespaceDeclaration.Name.ToString() != currentNamespace){
                    currentNamespace = namespaceDeclaration.Name.ToString();
                    listModelNamespaces.Add(currentNamespace);
                }
            }

            if(listModelNamespaces.Count > 0) {
                // Get first namespace
                var firstNamespace = listModelNamespaces.First();

                var projectNamespace = firstNamespace.Split('.')[0];

                source = source.Replace("@namespace", projectNamespace);
            }

            source += "}}";

            if(!string.IsNullOrEmpty(source))
                context.AddSource("GenerateRepoWrapper.g.cs", source);
        }
    }
    public void Initialize(GeneratorInitializationContext context)
    {

    }
}