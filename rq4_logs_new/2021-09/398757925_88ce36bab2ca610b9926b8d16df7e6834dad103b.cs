using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Moq;

using NUnit.Framework;

using Ralfred.Common.DataAccess.Context;
using Ralfred.Common.DataAccess.Entities;
using Ralfred.Common.DataAccess.Repositories;


namespace SecretsProvider.UnitTests.DataAccess.Repositories
{
	public class GroupRepository
	{
		[SetUp]
		public void Setup()
		{
			_secretContext = new Mock<IStorageContext<Secret>>();
			_groupContext = new Mock<IStorageContext<Group>>();
			_target = new Ralfred.Common.DataAccess.Repositories.GroupRepository(_groupContext.Object, _secretContext.Object);
		}

		[Test]
		public void ExistsTest()
		{
			// arrange
			const string name = "test";
			const string path = "";

			var mockGroup = new Group
			{
				Id = Guid.NewGuid(),
				Name = name,
				Path = path
			};

			_groupContext.Setup(x => x.Find(It.IsAny<Expression<Predicate<Group>>>())).Returns(mockGroup);

			// act
			var exists = _target.Exists(name, path);

			// assert
			Assert.AreEqual(exists, true);
			_groupContext.Verify(x => x.Find(It.IsAny<Expression<Predicate<Group>>>()), Times.Once);
		}

		[Test]
		public void NotExistsTest()
		{
			// arrange
			const string name = "test";
			const string path = "";

			_groupContext.Setup(x => x.Find(It.IsAny<Expression<Predicate<Group>>>())).Returns<Group?>(null);

			// act
			var exists = _target.Exists(name, path);

			// assert
			Assert.AreEqual(exists, false);
			_groupContext.Verify(x => x.Find(It.IsAny<Expression<Predicate<Group>>>()), Times.Once);
		}

		[Test]
		public void CreateGroupTest()
		{
			// arrange
			const string name = "test";
			const string path = "";

			var mockGroup = new Group
			{
				Id = Guid.NewGuid(),
				Name = name,
				Path = path
			};

			var secrets = new Dictionary<string, string>
			{
				{ "test", "test" },
				{ "test2", "test2" }
			};

			var files = new Dictionary<string, string>
			{
				{ "file", "file" }
			};

			_groupContext.Setup(x => x.Add(It.Is<Group>(y => y.Name == name && y.Path == path))).Returns(mockGroup);
			_secretContext.Setup(x => x.Add(It.IsAny<Secret>()));

			// act
			_target.CreateGroup(name, path, secrets, files);

			// assert
			_groupContext.Verify(x => x.Add(It.Is<Group>(y => y.Name == name && y.Path == path)), Times.Once);

			_secretContext.Verify(x => x.Add(It.IsAny<Secret>()),
				Times.Exactly(secrets.Keys.Count + files.Keys.Count));
		}

		private IGroupRepository _target;
		private Mock<IStorageContext<Secret>> _secretContext;
		private Mock<IStorageContext<Group>> _groupContext;
	}
}