using NUnit.Framework;
using RattrapDev.DDD.Core;
using Shouldly;

namespace Rattrap.DDD.Core.Tests
{
	[TestFixture]
	public class CompositeSpecificationTests
	{
		[Test]
		public void And_returns_true_for_two_matching_specifications()
		{
			var andSpecification = new IsOddNumber().And(new IsDivisibleByThree());
			andSpecification.IsSatisfiedBy(3).ShouldBeTrue();
		}

		[Test]
		public void And_returns_false_with_one_non_matching_sepcification()
		{
			var andSpecification = new IsOddNumber().And(new IsDivisibleByThree());
			andSpecification.IsSatisfiedBy(5).ShouldBeFalse();
		}

		[Test]
		public void Or_returns_true_for_one_matching_specification()
		{
			var orSpecification = new IsOddNumber().Or(new IsEvenNumber());
			orSpecification.IsSatisfiedBy(1).ShouldBeTrue();
			orSpecification.IsSatisfiedBy(2).ShouldBeTrue();
		}

		[Test]
		public void Or_returns_false_with_two_non_matchings_specifications()
		{
			var orSpecification = new IsEvenNumber().And(new IsDivisibleByThree());
			orSpecification.IsSatisfiedBy(1).ShouldBeFalse();
		}

		[Test]
		public void Not_returns_false_with_matching_specification()
		{
			var notSpecification = new IsDivisibleByThree().Not();
			notSpecification.IsSatisfiedBy(5).ShouldBeTrue();
		}

		[Test]
		public void AndNot_returns_true_for_one_matching_and_one_non_match()
		{
			var andNotSpecification = new IsEvenNumber().AndNot(new IsOddNumber());
			andNotSpecification.IsSatisfiedBy(4).ShouldBeTrue();
		}

		[Test]
		public void AndNot_returns_false_with_two_matching_specifications()
		{
			var andNotSpecification = new IsEvenNumber().AndNot(new IsDivisibleByThree());
			andNotSpecification.IsSatisfiedBy(6).ShouldBeFalse();
		}

		[Test]
		public void OrNot_returns_false_with_left_non_matching_and_right_matching()
		{
			var orNotSpecification = new IsEvenNumber().OrNot(new IsOddNumber());
			orNotSpecification.IsSatisfiedBy(3).ShouldBeFalse();
		}

		[Test]
		public void OrNot_returns_true_with_left_matching_and_right_non_matching()
		{
			var orNotSpecification = new IsEvenNumber().OrNot(new IsOddNumber());
			orNotSpecification.IsSatisfiedBy(4).ShouldBeTrue();
		}

		private class IsOddNumber : CompositeSpecification<int>
		{
			public override bool IsSatisfiedBy(int candidate)
			{
				return candidate % 2 == 1;
			}
		}

		private class IsEvenNumber : CompositeSpecification<int>
		{
			public override bool IsSatisfiedBy(int candidate)
			{
				return candidate % 2 == 0;
			}
		}

		private class IsDivisibleByThree : CompositeSpecification<int>
		{
			public override bool IsSatisfiedBy(int candidate)
			{
				return candidate % 3 == 0;
			}
		}
	}
}
