using BusinessLogic.Models;
using BusinessLogic.Repositories;
using BusinessLogic.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System.Collections.Generic;

namespace UnitTests.BusinessLogic.Services
{
	[TestClass]
	public class CardServiceTests
	{
		private Mock<IUnitOfWork> _Uow;
		private Mock<CardService> _Service;
		private Mock<IRepository<Card>> _cardRepositoryMock;
		private List<Card> _cardList;

		[TestInitialize]
		public void Setup()
		{
			this._Uow = new Mock<IUnitOfWork>();
			this._Service = new Mock<CardService>(this._Uow.Object);
			this._cardRepositoryMock = new Mock<IRepository<Card>>();

			this._cardList = new List<Card> 
			{
				new Card 
				{
					ID = 1,
					Name = "Bam!"
				},
				new Card
				{
					ID = 2,
					Name = "Arruga"
				},
				new Card
				{
					ID = 3,
					Name = "Kapow"
				},
				new Card 
				{
					ID = 4,
					Name = "Thud"
				}
			};
		}

		[TestMethod]
		public void TestThatGetAllMethodSortsAscending()
		{
			//--Arrange
			this._Uow.Setup(x => x.GetRepository<Card>()).Returns(this._cardRepositoryMock.Object);
			this._cardRepositoryMock.Setup(x => x.GetAll()).Returns(this._cardList);

			//--Act
			var results = this._Service.Object.GetAll();//true, "Name");
			//--Assert
			Assert.AreEqual("Arruga", results[0].Name);
		}

		[TestMethod]
		public void TestThatGetAllMethodSortsDescending()
		{
			//--Arrange
			this._Uow.Setup(x => x.GetRepository<Card>()).Returns(this._cardRepositoryMock.Object);
			this._cardRepositoryMock.Setup(x => x.GetAll()).Returns(this._cardList);

			//--Act
			var results = this._Service.Object.GetAll();//false, "Name");

			//--Assert
			Assert.AreEqual("Thud", results[0].Name);
		}
	}
}