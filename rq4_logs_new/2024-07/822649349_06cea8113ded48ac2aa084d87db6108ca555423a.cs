using FluentValidation;
using MediatR;
using Microservice.Customer.Api.Data.Repository.Interfaces;
using Microservice.Customer.Api.Helpers;
using Microservice.Customer.Api.Helpers.Exceptions;
using Microservice.Customer.Api.Helpers.Interfaces;
using Microservice.Customer.Api.MediatR.GetCustomer;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System.Reflection;

namespace Microservice.Customer.Api.Test.Unit;

[TestFixture]
public class GetCustomerMediatrTests
{
    private Mock<ICustomerRepository> customerRepositoryMock = new Mock<ICustomerRepository>();
    private Mock<ICustomerHttpAccessor> customerHttpAccessorMock = new Mock<ICustomerHttpAccessor>();
    private ServiceCollection services = new ServiceCollection();
    private ServiceProvider serviceProvider;
    private IMediator mediator;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        services.AddValidatorsFromAssemblyContaining<GetCustomerValidator>();
        services.AddMediatR(cfg => cfg.RegisterServicesFromAssemblies(typeof(GetCustomerQueryHandler).Assembly));
        services.AddTransient(typeof(IPipelineBehavior<,>), typeof(ValidatorBehavior<,>));
        services.AddScoped<ICustomerRepository>(sp => customerRepositoryMock.Object);
        services.AddScoped<ICustomerHttpAccessor>(sp => customerHttpAccessorMock.Object);
        services.AddAutoMapper(Assembly.GetAssembly(typeof(GetCustomerMapper)));

        serviceProvider = services.BuildServiceProvider();
        mediator = serviceProvider.GetRequiredService<IMediator>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        services.Clear();
        serviceProvider.Dispose();
    }

    [Test]
    public async Task Get_customer_return_customer()
    {
        var email = "ValidEmail@hotmail.com";
        var surname = "TestSurname";
        var firstName = "TestFirstName";

        var customerId = Guid.NewGuid();
        var customer = new Domain.Customer() { Id = customerId, Email = email, Surname = surname, FirstName = firstName };

        customerRepositoryMock
                .Setup(x => x.CustomerExistsAsync(customerId))
                .Returns(Task.FromResult(true));

        customerRepositoryMock
                .Setup(x => x.ByIdAsync(customerId))
                .Returns(Task.FromResult(customer));

        var getCustomerRequest = new GetCustomerRequest(customerId);

        var actualResult = await mediator.Send(getCustomerRequest);
        var expectedResult = new GetCustomerResponse(email, surname, firstName);

        Assert.That(actualResult.Email, Is.EqualTo(expectedResult.Email));
        Assert.That(actualResult.Surname, Is.EqualTo(expectedResult.Surname));
        Assert.That(actualResult.FirstName, Is.EqualTo(expectedResult.FirstName));
    }

    [Test]
    public void Get_customer_return_exception()
    {
        var customerId = Guid.NewGuid();

        customerRepositoryMock
                .Setup(x => x.CustomerExistsAsync(customerId))
                .Returns(Task.FromResult(false));

        customerHttpAccessorMock.Setup(x => x.CustomerId)
            .Returns(customerId);

        var getCustomerRequest = new GetCustomerRequest(Guid.NewGuid()); 

        var validationException = Assert.ThrowsAsync<NotFoundException>(async () =>
        {
            await mediator.Send(getCustomerRequest);
        });
         
        Assert.That(validationException.Message, Is.EqualTo("Customer not found."));
    }
}