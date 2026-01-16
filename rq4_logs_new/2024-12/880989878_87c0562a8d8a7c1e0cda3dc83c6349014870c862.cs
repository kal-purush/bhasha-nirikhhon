using Bogus;
using MarGate.Core.Persistence.Repository;
using MarGate.Core.Persistence.UnitOfWork;
using MarGate.Identity.Application.Handlers.Identity.Queries.GetUserById;
using MarGate.Identity.Domain.Entities;
using Moq;

namespace MarGate.Identity.Application.Tests;

public class GetUserByIdTests
{
    [Fact]
    public async Task Handle_ShouldReturnCorrectUser_WhenUserExists()
    {
        // Arrange
        var userId = 123456L; 
        var mockUnitOfWork = new Mock<IUnitOfWork>();
        var mockUserReadRepository = new Mock<IReadRepository<User>>();

        // Create a fake user using Bogus
        var faker = new Faker<User>()
            .RuleFor(u => u.Id, userId) 
            .RuleFor(u => u.FirstName, f => f.Name.FirstName())
            .RuleFor(u => u.LastName, f => f.Name.LastName())
            .RuleFor(u => u.EmailAddress, f => new EmailAddress(f.Internet.Email()))
            .RuleFor(u => u.PhoneNumber, f => new PhoneNumber(f.Phone.PhoneNumber()))
            .RuleFor(u => u.Address, f => new Address
            {
                Street = f.Address.StreetAddress(),
                City = f.Address.City(),
                Country = f.Address.Country()
            })
            .RuleFor(u => u.Balance, f => f.Finance.Amount())
            .RuleFor(u => u.BirthDate, f => f.Date.Past(24));  

        var user = faker.Generate(); 

        mockUserReadRepository
            .Setup(repo => repo.FirstOrDefaultAsync(It.IsAny<Func<User, bool>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(user);

        mockUnitOfWork
            .Setup(uow => uow.GetReadRepository<User>())
            .Returns(mockUserReadRepository.Object);

        var handler = new GetUserByIdQueryHandler(mockUnitOfWork.Object);
        var request = new GetUserByIdQueryRequest { Id = userId };

        // Act
        var result = await handler.Handle(request, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(userId, result.Id);  
        Assert.Equal(user.FirstName, result.FirstName);
        Assert.Equal(user.LastName, result.LastName);
        Assert.Equal(user.EmailAddress.Address, result.EmailAddress);
        Assert.Equal(user.PhoneNumber.Number, result.PhoneNumber);
        Assert.Equal($"{user.Address.Street}, {user.Address.City}, {user.Address.Country}", result.Address);
        Assert.Equal(user.Balance, result.Balance);
        Assert.Equal(user.BirthDate, result.BirthDate); 
    }

    [Fact]
    public async Task Handle_ShouldReturnNull_WhenUserDoesNotExist()
    {
        // Arrange
        var userId = 123456L;  
        var mockUnitOfWork = new Mock<IUnitOfWork>();
        var mockUserReadRepository = new Mock<IReadRepository<User>>();

        // Mocking the repository call to return null (no user found)
        mockUserReadRepository
            .Setup(repo => repo.FirstOrDefaultAsync(It.IsAny<Func<User, bool>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((User)null);

        mockUnitOfWork
            .Setup(uow => uow.GetReadRepository<User>())
            .Returns(mockUserReadRepository.Object);

        var handler = new GetUserByIdQueryHandler(mockUnitOfWork.Object);

        var request = new GetUserByIdQueryRequest { Id = userId };

        // Act
        var result = await handler.Handle(request, CancellationToken.None);

        // Assert
        Assert.Null(result);
    }
}