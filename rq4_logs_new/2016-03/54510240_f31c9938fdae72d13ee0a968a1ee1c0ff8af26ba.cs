using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HellBrick.Refactorings.ExpressionBodies;
using Xunit;

namespace HellBrick.Refactorings.Test
{
	public class ToBlockBodiedMemberTest
	{
		private readonly ToBlockBodyRefactoring _provider = new ToBlockBodyRefactoring();

		[Fact]
		public void VoidMethodIsConverted()
		{
			const string sourceCode = "void DoStuff() => Console.WriteLine( 42 );";
			const string expected =
@"void DoStuff()
{
	Console.WriteLine( 42 );
}";
			_provider.ShouldProvideRefactoring( sourceCode, expected );
		}

		[Fact]
		public void NonVoidMethodIsConverted()
		{
			const string sourceCode = "int Calc() => 42;";
			const string expected =
@"int Calc()
{
	return 42;
}";
			_provider.ShouldProvideRefactoring( sourceCode, expected );
		}

		[Fact]
		public void OperatorIsConverted()
		{
			const string sourceCode = "SomeStruct operator + ( SomeStruct s1, SomeStruct s2 ) => s1.Add( s2 );";
			const string expected =
@"SomeStruct operator + ( SomeStruct s1, SomeStruct s2 )
{
	return s1.Add( s2 );
}";
			_provider.ShouldProvideRefactoring( sourceCode, expected );
		}

		[Fact]
		public void PeopertyIsConverted()
		{
			const string sourceCode = "int Value => 42;";
			const string expected =
@"int Value
{
	get
	{
		return 42;
	}
}";
			_provider.ShouldProvideRefactoring( sourceCode, expected );
		}
	}
}