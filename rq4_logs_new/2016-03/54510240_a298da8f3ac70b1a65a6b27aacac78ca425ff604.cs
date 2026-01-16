using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HellBrick.Refactorings.StringInterpolation;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeRefactorings;
using Xunit;

namespace HellBrick.Refactorings.Test
{
	public class StringFormatToStringInterpolationTest
	{
		private StringFormatToStringInterpolationRefactoring _refactoringProvider = new StringFormatToStringInterpolationRefactoring();

		[Fact]
		public void CallWithoutAlignmentOrFormatIsConverted()
		{
			const string sourceCode = @"var x = System.String.Format( ""asdf {0} qwer"", 42 );";
			const string expectedCode = @"var x = $""asdf {42} qwer"";";
			_refactoringProvider.ShouldProvideRefactoring( sourceCode, expectedCode );
		}

		[Fact]
		public void BracesAreProperlyEscaped()
		{
			const string sourceCode = @"var x = System.String.Format( ""{{ {0} }}"", 42 );";
			const string expectedCode = @"var x = $""{{ {42} }}"";";
			_refactoringProvider.ShouldProvideRefactoring( sourceCode, expectedCode );
		}

		[Fact]
		public void CallWithFormatIsConverted()
		{
			const string sourceCode = @"var x = System.String.Format( ""{0:g2}"", 42 );";
			const string expectedCode = @"var x = $""{42:g2}"";";
			_refactoringProvider.ShouldProvideRefactoring( sourceCode, expectedCode );
		}

		[Fact]
		public void CallWithAlignmentIsConverted()
		{
			const string sourceCode = @"var x = System.String.Format( ""{0,5}"", 42 );";
			const string expectedCode = @"var x = $""{42,5}"";";
			_refactoringProvider.ShouldProvideRefactoring( sourceCode, expectedCode );
		}

		[Fact]
		public void CallWithAlignmentAndFormatIsConverted()
		{
			const string sourceCode = @"var x = System.String.Format( ""{0,5:g2}"", 42 );";
			const string expectedCode = @"var x = $""{42,5:g2}"";";
			_refactoringProvider.ShouldProvideRefactoring( sourceCode, expectedCode );
		}

		[Fact]
		public void CallWithoutConstantFormatIsNotConverted()
		{
			const string sourceCode = @"string f = ""{0}""; var x = System.String.Format( f, 42 );";
			_refactoringProvider.ShouldNotProvideRefactoring( sourceCode );
		}

		[Fact]
		public void CallWithIncorrectFormatIsNotConverted()
		{
			const string sourceCode = @"var x = System.String.Format( ""{0} {1}"", 42 );";
			_refactoringProvider.ShouldNotProvideRefactoring( sourceCode );
		}
	}
}