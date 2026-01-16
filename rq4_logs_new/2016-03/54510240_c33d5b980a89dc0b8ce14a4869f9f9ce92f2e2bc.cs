using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace HellBrick.Refactorings.ExpressionBodies
{
	public abstract class BaseMethodExpressionBodyHandler<TDeclaration> : IExpressionBodyHandler<TDeclaration>
		where TDeclaration : BaseMethodDeclarationSyntax
	{
		public bool CanConvertToExpression( TDeclaration declaration ) => true;
		public BlockSyntax GetBody( TDeclaration declaration ) => declaration.Body;
		public SyntaxNode GetRemovedNode( TDeclaration declaration ) => declaration.Body;

		public abstract string GetIdentifierName( TDeclaration declaration );
		public abstract TDeclaration ReplaceBodyWithExpressionClause( TDeclaration declaration, ArrowExpressionClauseSyntax arrow );
	}
}