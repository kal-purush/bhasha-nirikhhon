using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Moq;

using NUnit.Framework;

using Ralfred.Common.DataAccess.Context;
using Ralfred.Common.DataAccess.Entities;
using Ralfred.Common.DataAccess.Repositories;


namespace SecretsProvider.UnitTests.DataAccess.Repositories
{
	[TestFixture]
	public class SecretsRepositoryTests
	{
		[SetUp]
		public void Setup()
		{
			_secretContext = new Mock<IStorageContext<Secret>>();
			_groupContext = new Mock<IStorageContext<Group>>();
			_target = new SecretsRepository(_secretContext.Object, _groupContext.Object);
		}

		[Test]
		public void GetGroupSecretsTest()
		{
			// arrange
			var mockGroup = new Group
			{
				Id = Guid.NewGuid(),
				Name = "test",
				Path = ""
			};

			var mockSecret = new Secret
			{
				Id = Guid.NewGuid(),
				Name = "testSecret",
				Value = "test",
				GroupId = mockGroup.Id,
				IsFile = false
			};

			_groupContext.Setup(x => x.Get(It.IsAny<Expression<Predicate<Group>>>())).Returns(mockGroup);
			_secretContext.Setup(x => x.List(It.IsAny<Expression<Func<Secret, bool>>>())).Returns(new List<Secret> { mockSecret });

			// act
			var result = _target.GetGroupSecrets(mockGroup.Path, mockGroup.Name).ToList();

			// assert
			Assert.AreEqual(result.Count, 1);
			Assert.AreEqual(result[0].Id, mockSecret.Id);
			Assert.AreEqual(result[0].Name, mockSecret.Name);
			Assert.AreEqual(result[0].GroupId, mockSecret.GroupId);
			Assert.AreEqual(result[0].Value, mockSecret.Value);
			Assert.AreEqual(result[0].IsFile, mockSecret.IsFile);
		}

		private ISecretsRepository _target;
		private Mock<IStorageContext<Secret>> _secretContext;
		private Mock<IStorageContext<Group>> _groupContext;
	}
}