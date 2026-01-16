using Bank.Permissions.Services;

using Microsoft.Extensions.DependencyInjection;

using Moq;

namespace Bank.UserService.Test.Integration.Services;

public class TestAuthorizationServiceFactory(IServiceScopeFactory serviceScopeFactory) : IAuthorizationServiceFactory
{
    public Guid                           UserId      { set; get; }
    public Permissions.Domain.Permissions Permissions { set; get; }
    
    public IAuthorizationService AuthorizationService => CreateAuthorizationService();

    private IAuthorizationService CreateAuthorizationService()
    {
        var mockAuthorizationService = new Mock<IAuthorizationService>();

        mockAuthorizationService.SetupGet(authorizationService => authorizationService.UserId)
                                .Returns(UserId);
        
        mockAuthorizationService.SetupGet(authorizationService => authorizationService.Permissions)
                                .Returns(Permissions);

        return mockAuthorizationService.Object;
    }
}