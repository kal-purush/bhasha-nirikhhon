using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;
using System.IO;

namespace SourceBuilding.Core
{
    public class ScriptGenerator
    {
        public string BuildMainScript(string @namespace, string contextName, string outFilePath, string propertyName)
        {
            var tree = CompilationUnit()
                .WithUsings(
                    List(new[] {
                        UsingDirective(
                                IdentifierName("Microsoft.EntityFrameworkCore"))
                            .WithUsingKeyword(
                                Token(
                                    TriviaList(
                                        Trivia(
                                            ReferenceDirectiveTrivia(
                                                Literal("nuget:NetStandard.Library,1.6.1"),
                                                true)),
                                        Trivia(
                                            ReferenceDirectiveTrivia(
                                                Literal("nuget:Microsoft.EntityFrameworkCore.SqlServer,1.1.2"),
                                                true)),
                                        GetLibraryTrivia(outFilePath)),
                                    SyntaxKind.UsingKeyword,
                                    TriviaList())),
                    UsingDirective(IdentifierName(@namespace))}))
                .WithMembers(BuildMembers(contextName, propertyName))
                .NormalizeWhitespace();


            var descendants = tree.DescendantNodesAndTokens();
            var triviaDesc = tree.DescendantTrivia();
            var scriptText = tree.GetText().ToString();
            return scriptText;
        }

        private SyntaxTrivia GetLibraryTrivia(string outFilePath)
        {
            SyntaxTrivia trivia;

            var isScript = new FileInfo(outFilePath).Extension == ".csx";
            if (isScript)
                trivia = Trivia(LoadDirectiveTrivia(Literal(
                            $@"""{outFilePath}""", outFilePath), true));
            else
                trivia = Trivia(ReferenceDirectiveTrivia(Literal(
                            $@"""{outFilePath}""", outFilePath), true));

            return trivia;
        }

        public string GetPropertyName(string sourceText)
        {
            var root = CSharpSyntaxTree.ParseText(sourceText).GetCompilationUnitRoot();
            var dbsetPropertyNode = root.DescendantNodes().OfType<PropertyDeclarationSyntax>()
                .FirstOrDefault(n => (n.Type as GenericNameSyntax)?.Identifier.Text == "DbSet");
            var propertyName = dbsetPropertyNode.Identifier.Text;
            return propertyName;
        }

        private SyntaxList<MemberDeclarationSyntax> BuildMembers(string contextName, string propertyName)
        {
            return List(
                new MemberDeclarationSyntax[]{
                    FieldDeclaration(
                        VariableDeclaration(
                                IdentifierName("var"))
                            .WithVariables(
                                SingletonSeparatedList(
                                    VariableDeclarator(
                                            Identifier("context"))
                                        .WithInitializer(
                                            EqualsValueClause(
                                                ObjectCreationExpression(
                                                        IdentifierName(contextName))
                                                    .WithArgumentList(
                                                        ArgumentList())))))),
                    FieldDeclaration(
                        VariableDeclaration(
                                IdentifierName("var"))
                            .WithVariables(
                                SingletonSeparatedList(
                                    VariableDeclarator(
                                            Identifier("query"))
                                        .WithInitializer(
                                            EqualsValueClause(
                                                InvocationExpression(
                                                        MemberAccessExpression(
                                                            SyntaxKind.SimpleMemberAccessExpression,
                                                            MemberAccessExpression(
                                                                SyntaxKind.SimpleMemberAccessExpression,
                                                                IdentifierName("context"),
                                                                IdentifierName(propertyName)),
                                                            IdentifierName("Take")))
                                                    .WithArgumentList(
                                                        ArgumentList(
                                                            SingletonSeparatedList(
                                                                Argument(
                                                                    LiteralExpression(
                                                                        SyntaxKind.NumericLiteralExpression,
                                                                        Literal(1))))))))))),
                   FieldDeclaration(
                                VariableDeclaration(
                                    IdentifierName("var"))
                                .WithVariables(
                                    SingletonSeparatedList(
                                        VariableDeclarator(
                                            Identifier("list"))
                                        .WithInitializer(
                                            EqualsValueClause(
                                                InvocationExpression(
                                                    MemberAccessExpression(
                                                        SyntaxKind.SimpleMemberAccessExpression,
                                                        IdentifierName("query"),
                                                        IdentifierName("ToList")))))))),
                   GlobalStatement(ExpressionStatement(
                                InvocationExpression(
                                    MemberAccessExpression(
                                        SyntaxKind.SimpleMemberAccessExpression,
                                        IdentifierName("list"),
                                        IdentifierName("ForEach")))
                                .WithArgumentList(
                                    ArgumentList(
                                        SingletonSeparatedList(
                                            Argument(
                                                IdentifierName("WriteLine")))))))
                });
        }
    }
}