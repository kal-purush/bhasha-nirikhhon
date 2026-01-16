using System;
using NUnit.Framework;
using RattrapDev.DDD.Core.Identifier;
using Shouldly;

namespace Rattrap.DDD.Core.Tests
{
	[TestFixture]
	public class GuidIdentifierTests
	{
		[Test]
		public void Constructor_generates_new_guid()
		{
			var identifier = new TestGuidIdentifier();
			identifier.Id.ShouldNotBe(Guid.Empty);
		}

		[Test]
		public void Constructor_reconstitutes_with_guid()
		{
			var id = Guid.NewGuid();
			var identifier = new TestGuidIdentifier(id);
			identifier.Id.ShouldBe(id);
		}

		[Test]
		public void Equals_different_references_same_id_are_still_equal()
		{
			var id = Guid.NewGuid();
			var identifier1 = new TestGuidIdentifier(id);
			var identifier2 = new TestGuidIdentifier(id);

			identifier1.Equals(identifier2).ShouldBeTrue();
		}

		[Test]
		public void Equals_same_reference_are_equal()
		{
			var id = Guid.NewGuid();
			var identifier1 = new TestGuidIdentifier(id);
			var identifier2 = identifier1;

			identifier1.Equals(identifier2).ShouldBeTrue();
		}

		[Test]
		public void Equals_different_references_different_id_are_not_equal()
		{
			var identifier1 = new TestGuidIdentifier(Guid.NewGuid());
			var identifier2 = new TestGuidIdentifier(Guid.NewGuid());

			identifier1.Equals(identifier2).ShouldBeFalse();
		}

		private class TestGuidIdentifier : GuidIdentifierBase
		{
			public TestGuidIdentifier() : base() {}

			public TestGuidIdentifier(Guid identifier)
				: base(identifier)
			{
				
			}
		}
	}
}
