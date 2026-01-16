using System;
using System.Linq;
using System.Linq.Expressions;
using NUnit.Framework;
using RattrapDev.DDD.Core;
using Shouldly;

namespace Rattrap.DDD.Core.Tests
{
	[TestFixture]
	public class LinqSpecificationTests
	{
		[Test]
		public void LinqExpression_returns_matching_number()
		{
			var evenOrDivisibleByThree = new IsEvenNumber().Or(new IsDivisibleByThree());
			int[] numbers = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
			var matchingNumbers = numbers.Where(evenOrDivisibleByThree.ToFunc());
			matchingNumbers.ShouldContain(2);
			matchingNumbers.ShouldContain(3);
			matchingNumbers.ShouldContain(4);
			matchingNumbers.ShouldContain(6);
			matchingNumbers.ShouldContain(8);
			matchingNumbers.ShouldContain(9);
			matchingNumbers.ShouldContain(10);
		}

		private class IsEvenNumber : CompositeSpecification<int>
		{
			public override Expression<Func<int, bool>> AsExpression()
			{
				return ((x) => x % 2 == 0);
			}
		}

		private class IsDivisibleByThree : CompositeSpecification<int>
		{
			public override Expression<Func<int, bool>> AsExpression()
			{
				return ((x) => x % 3 == 0);
			}
		}
	}
}
