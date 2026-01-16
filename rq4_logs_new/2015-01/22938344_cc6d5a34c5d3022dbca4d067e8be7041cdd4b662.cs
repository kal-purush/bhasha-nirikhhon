using BusinessLogic.Models;
using BusinessLogic.Repositories;
using BusinessLogic.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System.Collections.Generic;

namespace UnitTests.BusinessLogic.Services
{
	[TestClass]
	public class PlayerServiceTests
	{
		private Mock<IUnitOfWork> _Uow;
		private Mock<PlayerService> _Service;
		private Mock<IRepository<Player>> _playerRepositoryMock;
		private List<Player> _playerList;

		[TestInitialize]
		public void Setup()
		{
			this._Uow = new Mock<IUnitOfWork>();
			this._Service = new Mock<PlayerService>(this._Uow.Object);
			this._playerRepositoryMock = new Mock<IRepository<Player>>();

			this._playerList = new List<Player>
			{
				new Player
				{
					ID = 1,
					Name = "Smitty Werbenjagermanjensen",
					IsActive = true
				},
				new Player
				{
					ID = 2,
					Name = "Neesons",
					IsActive = true
				},		
				new Player
				{
					ID = 3,
					Name = "Willis",
					IsActive = true
				},
				new Player
				{
					ID = 4,
					Name = "Hank Moody",
					IsActive = true
				}
			};
		}

		[TestMethod]
		public void TestThatGetAllMethodOrdersAscending()
		{
			//--Arrange
			this._Uow.Setup(x => x.GetRepository<Player>()).Returns(this._playerRepositoryMock.Object);
			this._playerRepositoryMock.Setup(x => x.GetAll()).Returns(this._playerList);

			//--Act
			var results = this._Service.Object.GetAll(true);

			//--Assert
			Assert.AreEqual("Hank Moody", results[0].Name);
		}

		[TestMethod]
		public void TestThatGetAllMethodOrdersDescending()
		{
			//--Arrange
			this._Uow.Setup(x => x.GetRepository<Player>()).Returns(this._playerRepositoryMock.Object);
			this._playerRepositoryMock.Setup(x => x.GetAll()).Returns(this._playerList);

			//--Act
			var results = this._Service.Object.GetAll(false);

			//--Assert
			Assert.AreEqual("Willis", results[0].Name);
		}
	}
}