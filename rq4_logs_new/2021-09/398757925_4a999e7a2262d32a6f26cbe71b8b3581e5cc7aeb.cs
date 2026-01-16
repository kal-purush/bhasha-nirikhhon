using System;

using NUnit.Framework;

using Ralfred.Common.DataAccess.Context;
using Ralfred.Common.DataAccess.Entities;


namespace SecretsProvider.UnitTests.DataAccess.Context
{
	[TestFixture]
	public class InMemoryStorageContext
	{
		[SetUp]
		public void Setup()
		{
			_target = new InMemoryStorageContext<TestEntity>();
		}

		[Test]
		public void EmptyOnInitTest()
		{
			// arrange

			// act
			var entities = _target.List();

			//assert
			Assert.IsEmpty(entities);
		}

		[Test]
		public void AddTest()
		{
			// arrange
			var newEntity = new TestEntity
			{
				Name = "test",
				Age = 0
			};

			// act
			var entity = _target.Add(newEntity);

			// assert
			Assert.AreEqual(entity.Name, newEntity.Name);
			Assert.AreEqual(entity.Age, newEntity.Age);
			Assert.AreNotEqual(entity.Id, Guid.Empty);
		}

		private IStorageContext<TestEntity> _target;
	}

	public record TestEntity : Entity
	{
		public string Name { get; set; }

		public uint Age { get; set; }
	}
}