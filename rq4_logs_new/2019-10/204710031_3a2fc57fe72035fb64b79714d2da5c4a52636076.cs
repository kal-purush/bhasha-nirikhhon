using LPChat.Domain.Entities;
using LPChat.Domain.Interfaces;
using LPChat.Infrastructure.Services;
using Moq;
using NUnit.Framework;
using System;
using System.Threading.Tasks;

namespace Tests
{
	public class PersonInfoServiceTests
	{
		private readonly Mock<IRepositoryManager> repoManager;
		private readonly Mock<IRepository<Person>> personRepository;

		private readonly Guid singleUserTestId;

		public PersonInfoServiceTests()
		{
			repoManager = new Mock<IRepositoryManager>();
			personRepository = new Mock<IRepository<Person>>();
			singleUserTestId = new Guid();
		}

		[SetUp]
		public void Setup()
		{
			var id = singleUserTestId;

			personRepository.Setup(x => x.FindById(It.IsAny<Guid>()))
				.Returns(Task.FromResult(new Person
				{
					ID = id,
					FirstName = "Oleg",
					LastName = "Gonchar",
					Username = "hanchar.aleh@gmail.com",
					LastUpdatedUtcDate = DateTime.UtcNow
				}));

			repoManager.Setup(x => x.GetRepository<Person>()).Returns(personRepository.Object);
		}

		[Test]
		public void GetOneUserById()
		{
			var personService = new PersonInfoService(repoManager.Object);
			var person = personService.GetOneAsync(singleUserTestId).Result;
			Assert.AreEqual("hanchar.aleh@gmail.com", person.Username);
			Assert.AreEqual("Oleg", person.FirstName);
			Assert.AreEqual("Gonchar", person.LastName);
		}
	}
}