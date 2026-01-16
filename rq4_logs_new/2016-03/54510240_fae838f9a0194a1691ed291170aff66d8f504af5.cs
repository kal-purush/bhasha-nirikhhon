using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HellBrick.Refactorings.ExpressionBodies;
using Xunit;

namespace HellBrick.Refactorings.Test
{
	public class ToExpressionBodiedOperatorTest
	{
		private ToExpressionBodiedOperatorRefactoring _provider = new ToExpressionBodiedOperatorRefactoring();

		[Fact]
		public void MultipleLineOperatorIsIgnored()
		{
			const string sourceCode =
@"SomeStruct operator + Add( SomeStruct x, SomeStruct y )
{
	SomeStruct result = x.Add( y );
	return result;
}";
			_provider.ShouldNotProvideRefactoring( sourceCode );
		}

		[Fact]
		public void SingleLineThrowingOperatorIsIgnored()
		{
			const string sourceCode = "SomeStruct operator + Add( SomeStruct x, SomeStruct y ) { throw new Exception(); }";
			_provider.ShouldNotProvideRefactoring( sourceCode );
		}

		[Fact]
		public void ExpressionBodiedOperatorIsIgnored()
		{
			const string sourceCode = "SomeStruct operator + Add( SomeStruct x, SomeStruct y ) => x.Add( y );";
			_provider.ShouldNotProvideRefactoring( sourceCode );
		}

		[Fact]
		public void SingleLineReturningOperatorIsConverted()
		{
			const string sourceCode = "SomeStruct operator + Add( SomeStruct x, SomeStruct y ) { return x.Add( y ); }\r\n";
			const string expected = "SomeStruct operator + Add( SomeStruct x, SomeStruct y ) => x.Add( y );\r\n";
			_provider.ShouldProvideRefactoring( sourceCode, expected );
		}
	}
}