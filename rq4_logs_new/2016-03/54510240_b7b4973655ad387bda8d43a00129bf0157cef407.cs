using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HellBrick.Refactorings.VarConversions;
using Xunit;

namespace HellBrick.Refactorings.Test
{
	public class VarToExplicitTypeTest
	{
		private VarConversionRefactoring _provider = new VarConversionRefactoring();

		[Fact]
		public void AnonymousTypeVariableIsIgnored()
		{
			const string autoTyped = "void M() { var x = new { Value = 42 }; }";
			_provider.ShouldNotProvideRefactoring( autoTyped );
		}

		[Fact]
		public void LocalVariableIsRoundTripped()
		{
			const string explicitlyTyped = "void M() { int x = 42; }";
			const string autoTyped = "void M() { var x = 42; }";
			_provider.ShouldProvideRefactoring( explicitlyTyped, autoTyped );
			_provider.ShouldProvideRefactoring( autoTyped, explicitlyTyped );
		}

		[Fact]
		public void ForeachVariableIsRoundTripped()
		{
			const string explicitlyTyped = "void M() { foreach ( int x in new[] { 1, 2, 3 } ) { Console.WriteLine( x ); } }";
			const string autoTyped = "void M() { foreach ( var x in new[] { 1, 2, 3 } ) { Console.WriteLine( x ); } }";
			_provider.ShouldProvideRefactoring( explicitlyTyped, autoTyped );
			_provider.ShouldProvideRefactoring( autoTyped, explicitlyTyped );
		}

		[Fact]
		public void UsingVariableIsRoundTripped()
		{
			const string explicitlyTyped = "void M() { using ( System.IO.FileStream stream = System.IO.File.Open( file ) ) { DoStuff( stream ); } }";
			const string autoTyped = "void M() { using ( var stream = System.IO.File.Open( file ) ) { DoStuff( stream ); } }";
			_provider.ShouldProvideRefactoring( explicitlyTyped, autoTyped );
			_provider.ShouldProvideRefactoring( autoTyped, explicitlyTyped );
		}
	}
}