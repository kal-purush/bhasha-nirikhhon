using System;
using System.Linq;

using AutoFixture;

using FluentAssertions;

using NUnit.Framework;

using Ralfred.Common.DataAccess.Context;
using Ralfred.Common.DataAccess.Entities;


namespace SecretsProvider.UnitTests.DataAccess.Context
{
	[TestFixture]
	public class InMemoryStorageContextTests
	{
		[SetUp]
		public void Setup()
		{
			_fixture = new Fixture();

			_target = new InMemoryStorageContext<TestEntity>();
		}

		[Test]
		public void EmptyOnInitTest()
		{
			// arrange

			// act
			var entities = _target.List();

			//assert
			entities.Should().BeEmpty();
		}

		[Test]
		public void AddTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();

			// act
			var entity = _target.Add(newEntity);

			// assert
			entity.Age.Should().Be(newEntity.Age);
			entity.Name.Should().Be(newEntity.Name);
			entity.Id.Should().NotBe(Guid.Empty);
		}

		[Test]
		public void AddExistingIdTest()
		{
			// arrange
			var newEntity = _fixture.Create<TestEntity>();

			var entity = _target.Add(newEntity);

			var newEntity2 = _fixture.Build<TestEntity>().With(x => x.Id, entity.Id).Create();

			// act

			// assert
			Assert.Throws<ArgumentException>(() => _target.Add(newEntity2));
		}

		[Test]
		public void AddWithIdTest()
		{
			// arrange
			var newEntity = _fixture.Create<TestEntity>();

			// act
			var entity = _target.Add(newEntity);

			// assert
			entity.Should().BeEquivalentTo(newEntity);
		}

		[Test]
		public void GetTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();

			var entity = _target.Add(newEntity);

			// act
			var found = _target.Get(x => x.Id == entity.Id);

			// assert
			found.Should().BeEquivalentTo(entity);
		}

		[Test]
		public void GetNotFoundTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();

			_target.Add(newEntity);

			// act

			// assert
			Assert.Throws<Exception>(() => _target.Get(x => x.Name == _fixture.Create<string>()));
		}

		[Test]
		public void FindTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();

			var entity = _target.Add(newEntity);

			// act
			var found = _target.Find(x => x.Id == entity.Id);

			// assert
			found.Should().NotBeNull();
			found.Should().BeEquivalentTo(entity);
		}

		[Test]
		public void FindNullTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();

			_target.Add(newEntity);

			// act
			var found = _target.Find(x => x.Name == _fixture.Create<string>());

			// assert
			found.Should().BeNull();
		}

		[Test]
		public void ListTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();
			var entity = _target.Add(newEntity);

			// act
			var list = _target.List().ToList();

			// assert
			list.Should().HaveCount(1);
			list.Single().Should().BeEquivalentTo(entity);
		}

		[Test]
		public void FilteredListTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();
			var newEntity2 = _fixture.Build<TestEntity>().Without(x => x.Id).Create();
			var entity = _target.Add(newEntity);

			_target.Add(newEntity2);

			// act
			var found = _target.List(x => x.Name == entity.Name).ToList();

			// assert
			found.Should().HaveCount(1);
			found.Single().Should().BeEquivalentTo(entity);
		}

		[Test]
		public void EmptyListTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();
			var newEntity2 = _fixture.Build<TestEntity>().Without(x => x.Id).Create();

			_target.Add(newEntity);
			_target.Add(newEntity2);

			// act
			var found = _target.List(x => x.Name == _fixture.Create<string>()).ToList();

			// assert
			found.Should().BeEmpty();
		}

		[Test]
		public void DeleteTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();
			var entity = _target.Add(newEntity);

			// act
			var deleted = _target.Delete(x => x.Id == entity.Id).ToList();

			// assert
			deleted.Should().HaveCount(1);
			deleted.Single().Should().BeEquivalentTo(entity);
		}

		[Test]
		public void DeleteManyTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().With(x => x.Age, (uint) 0).Create();
			var newEntity2 = _fixture.Build<TestEntity>().With(x => x.Age, (uint) 0).Create();

			_target.Add(newEntity);
			_target.Add(newEntity2);

			// act
			var deleted = _target.Delete(x => x.Age == 0).ToList();

			// assert
			deleted.Should().HaveCount(2);
		}

		public void DeleteNotFoundTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();
			_target.Add(newEntity);

			// act
			var deleted = _target.Delete(x => x.Name == _fixture.Create<string>());

			// assert
			deleted.Should().HaveCount(0);
		}

		[Test]
		public void UpdateTest()
		{
			// arrange
			var newEntity = _fixture.Build<TestEntity>().Without(x => x.Id).Create();

			var entity = _target.Add(newEntity);
			var updatedEntity = _fixture.Build<TestEntity>().With(x => x.Id, entity.Id).Create();

			// act
			var updated = _target.Update(updatedEntity);

			// assert
			updated.Should().BeEquivalentTo(updatedEntity);
		}

		[Test]
		public void UpdateWithoutIdTest()
		{
			// arrange
			var entityId = _fixture.Create<Guid>();
			var newEntity = _fixture.Build<TestEntity>().With(x => x.Id, entityId).Create();
			var updatedEntity = _fixture.Build<TestEntity>().With(x => x.Id, entityId).Create();

			_target.Add(newEntity);

			// act
			var entity = _target.Update(updatedEntity);

			// assert
			entity.Should().NotBeNull();
		}

		private IFixture _fixture;

		private IStorageContext<TestEntity> _target;
	}

	public record TestEntity : Entity
	{
		public string Name { get; init; }

		public uint Age { get; init; }
	}
}