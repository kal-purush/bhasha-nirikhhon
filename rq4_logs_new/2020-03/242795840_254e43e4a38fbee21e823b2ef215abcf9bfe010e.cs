using System.Collections.Immutable;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace TestAnalyzers
{
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public class AssertEqualsAnalyzer : DiagnosticAnalyzer
    {
        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.Analyze | GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(AnalyzeMethod, SyntaxKind.InvocationExpression);
        }

        private static void AnalyzeTree(SyntaxNodeAnalysisContext context)
        {
            var expression = (InvocationExpressionSyntax)context.Node;
            if (!(expression.Expression is MemberAccessExpressionSyntax memberAccess))
                return;

            if (!(memberAccess.Expression is IdentifierNameSyntax assertIdentifier) || assertIdentifier.ToString() != assertName)
                return;

            if (memberAccess.Name.ToString() != methodName)
                return;

            context.ReportDiagnostic(Diagnostic.Create(rule, context.Node.GetLocation()));
        }

        private static void AnalyzeType(SyntaxNodeAnalysisContext context)
        {
            var expression = (InvocationExpressionSyntax)context.Node;
            if (!(expression.Expression is MemberAccessExpressionSyntax memberAccess) || memberAccess.Name.ToString() != methodName)
                return;

            var type = context.SemanticModel.GetTypeInfo(memberAccess.Expression).Type;
            if (type?.ToString() != assertFullName)
                return;

            context.ReportDiagnostic(Diagnostic.Create(rule, context.Node.GetLocation()));
        }

        private static void AnalyzeMethod(SyntaxNodeAnalysisContext context)
        {
            var expression = (InvocationExpressionSyntax)context.Node;

            var symbol = context.SemanticModel.GetSymbolInfo(expression.Expression).Symbol;
            if (symbol == null || !(symbol is IMethodSymbol methodSymbol))
                return;

            if (methodSymbol.ContainingType?.ToString() != assertFullName || methodSymbol.Name != methodName)
                return;

            context.ReportDiagnostic(Diagnostic.Create(rule, context.Node.GetLocation()));
        }

        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(rule);

        public const string DiagnosticId = "NoNUnitAssert";

        private const string assertFullName = "NUnit.Framework.Assert";
        private const string assertName = "Assert";
        private const string methodName = "AreEqual";

        private static readonly DiagnosticDescriptor rule = new DiagnosticDescriptor(
            DiagnosticId, "AnalyzerTitle", "Следует использовать FluentAssertions",
            "TestAnalyzers", DiagnosticSeverity.Warning, isEnabledByDefault : true);
    }
}