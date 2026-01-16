using System;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

using SolutionAnalyzer.Common;

namespace SolutionAnalyzer.CodeVisitors
{
    /// <summary>
    /// Class CsCodeClassFinderVisitor.
    /// Used to calculate root classes into the file/compilation unit
    /// Implements the <see cref="SolutionAnalyzer.Common.ISyntaxNodeVisitor" />
    /// </summary>
    /// <seealso cref="SolutionAnalyzer.Common.ISyntaxNodeVisitor" />
    internal class CsCodeClassFinderVisitor : ISyntaxNodeVisitor
    {
        /// <summary>
        /// The class count inside the compilation unit/file
        /// </summary>
        private int _classCount;

        /// <summary>
        /// Determines whether is allowed to visit the specified syntax node.
        /// </summary>
        /// <param name="syntaxNode">The syntax node.</param>
        /// <returns><c>true</c> if is allowed to visit the specified syntax node; otherwise, <c>false</c>.</returns>
        public bool IsAllowedToVisit(SyntaxNode syntaxNode)
        {
            return syntaxNode.IsKind(SyntaxKind.ClassDeclaration);
        }

        /// <summary>
        /// Visits the specified syntax node.
        /// </summary>
        /// <param name="syntaxNode">The syntax node.</param>
        /// <param name="syntaxNodeWalker">The syntax node walker.</param>
        public void Visit(SyntaxNode syntaxNode, SyntaxNodeWalker syntaxNodeWalker)
        {
            ClassDeclarationSyntax classNode = syntaxNode as ClassDeclarationSyntax;
            Type parentType = classNode?.Parent?.GetType();
            if (parentType == typeof(CompilationUnitSyntax) || parentType == typeof(NamespaceDeclarationSyntax))
            {
                _classCount++;
            }
        }

        /// <summary>
        /// Gets the class count inside the analyzed file
        /// </summary>
        /// <value>The class count.</value>
        public int ClassCount
        {
            get
            {
                return _classCount;
            }
        }
    }
}