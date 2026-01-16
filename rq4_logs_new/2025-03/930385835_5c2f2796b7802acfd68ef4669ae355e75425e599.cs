using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Accounts.Application.Commands.RefreshTokens;
using PetFamily.Accounts.Contracts.Responses;
using PetFamily.Accounts.Domain.DataModels;
using PetFamily.Core;
using PetFamily.Core.Abstractions;
using PetFamily.IntegrationTests.Accounts.Heritage;
using PetFamily.IntegrationTests.General;

namespace PetFamily.IntegrationTests.Accounts.HandlersTests;

public class RefreshTokensHandlerTests : AccountsTestsBase
{
    private readonly ICommandHandler<LoginResponse, RefreshTokensCommand> _sut;

    public RefreshTokensHandlerTests(AccountsTestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider.GetRequiredService<ICommandHandler<LoginResponse, RefreshTokensCommand>>();
    }

    [Fact]
    public async Task RefreshTokens_success_should_return_new_access_and_refresh_tokens()
    {
        // arrange
        var EMAIL = "test@mail.com";
        var USERNAME = "testUserName";
        var PASSWORD = "Password121314s.";

        var user = await DataGenerator.SeedUserAsync(USERNAME, EMAIL, PASSWORD, UserManager, RoleManager);
        var accessToken = await TokenProvider.GenerateAccessToken(user, CancellationToken.None);
        var refreshToken = await TokenProvider.GenerateRefreshToken(user, accessToken.Jti, CancellationToken.None);

        var command = new RefreshTokensCommand(accessToken.AccessToken, refreshToken);

        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        
        // new session replaced the old and not just added to the sessions list
        AccountsDbContext.RefreshSessions.Count().Should().Be(1);
        
        // returned tokens should not be the same as they were before handler was executed
        result.Value.AccessToken.Should().NotBeEquivalentTo(accessToken.AccessToken);
        result.Value.RefreshToken.Should().NotBe(refreshToken);
        
        // refresh session was filled with data returned from handler
        var userClaims = await TokenProvider.GetUserClaims(result.Value.AccessToken);
        var userJtiString = userClaims.Value.FirstOrDefault(c => c.Type == CustomClaims.Jti).Value;
        
        var record = await AccountsDbContext.RefreshSessions.FirstOrDefaultAsync(
            rs => rs.UserId == user.Id &&
                  rs.Jti.ToString() == userJtiString &&
                  rs.RefreshToken == result.Value.RefreshToken);
        
        record.Should().NotBeNull();

    }
    
    [Fact]
    public async Task RefreshTokens_failure_should_return_error_because_refresh_token_lifetime_is_expired()
    {
        // arrange
        var EMAIL = "test@mail.com";
        var USERNAME = "testUserName";
        var PASSWORD = "Password121314s.";

        var user = await DataGenerator.SeedUserAsync(USERNAME, EMAIL, PASSWORD, UserManager, RoleManager);
        var accessToken = await TokenProvider.GenerateAccessToken(user, CancellationToken.None);
        
        // creating session with expired lifetime
        var refreshToken = await GenerateRefreshToken(
            user,
            accessToken.Jti,
            DateTime.UtcNow.AddDays(-2),
            CancellationToken.None);

        var command = new RefreshTokensCommand(accessToken.AccessToken, refreshToken);

        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsFailure.Should().BeTrue();
    }
    
    [Fact]
    public async Task RefreshTokens_failure_should_return_error_because_jti_doesnt_match()
    {
        // arrange
        var EMAIL = "test@mail.com";
        var USERNAME = "testUserName";
        var PASSWORD = "Password121314s.";

        var user = await DataGenerator.SeedUserAsync(USERNAME, EMAIL, PASSWORD, UserManager, RoleManager);
        var accessToken = await TokenProvider.GenerateAccessToken(user, CancellationToken.None);
        var refreshToken = await TokenProvider.GenerateRefreshToken(user, Guid.NewGuid(), CancellationToken.None);

        var command = new RefreshTokensCommand(accessToken.AccessToken, refreshToken);

        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsFailure.Should().BeTrue();
    }
    
    private async Task<Guid> GenerateRefreshToken(
        User user,
        Guid accessTokenJti,
        DateTime expiresIn,
        CancellationToken cancellationToken)
    {
        var refreshSession = new RefreshSession
        {
            User = user,
            CreatedAt = DateTime.UtcNow,
            ExpiresIn = expiresIn,
            Jti = accessTokenJti,
            RefreshToken = Guid.NewGuid()
        };

        await AccountsDbContext.AddAsync(refreshSession);
        await AccountsDbContext.SaveChangesAsync(cancellationToken);

        return refreshSession.RefreshToken;
    }
}