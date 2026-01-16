using System;
using NUnit.Framework;
using RattrapDev.DDD.Core.Term;

namespace Rattrap.DDD.Core.Tests
{
	[TestFixture]
	public class ValueObjectAttributionTests
	{
		[Test]
		public void Attribution_sets_name_and_definition()
		{
			var attributes = typeof(TestValueObject).GetCustomAttributes(true);
			var valueObjectTerm = (ValueObject)attributes[0];
			Assert.That(valueObjectTerm.Name, Is.EqualTo("Value"));
			Assert.That(valueObjectTerm.Definition, Is.EqualTo("Definition"));
		}

		[ValueObject("Value", "Definition")]
		private class TestValueObject 
		{
		}
	}
}
