using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace HellBrick.Refactorings.ExpressionBodies
{
	public static class ExpressionBodyHandlerExtensions
	{
		public static string GetMemberName<TDeclaration>( this IExpressionBodyHandler<TDeclaration> handler, TDeclaration declaration, SemanticModel semanticModel )
			where TDeclaration : MemberDeclarationSyntax
			=> semanticModel.GetDeclaredSymbol( declaration )?.Name
			?? handler.GetIdentifierName( declaration );
	}
}