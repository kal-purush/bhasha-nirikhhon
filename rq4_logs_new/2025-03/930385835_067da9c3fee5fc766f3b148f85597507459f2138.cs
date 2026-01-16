using CSharpFunctionalExtensions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Accounts.Application.Commands.Login;
using PetFamily.Accounts.Application.Dto;
using PetFamily.Accounts.Application.Queries.GetUserById;
using PetFamily.Core.Abstractions;
using PetFamily.IntegrationTests.Accounts.Heritage;
using PetFamily.IntegrationTests.General;
using PetFamily.SharedKernel.CustomErrors;

namespace PetFamily.IntegrationTests.Accounts.HandlersTests;

public class GetUserInfoByIdTests : AccountsTestsBase
{
    private readonly IQueryHandler<Result<UserInfoDto, ErrorList>, GetUserInfoByIdQuery> _sut;

    public GetUserInfoByIdTests(AccountsTestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider
            .GetRequiredService<IQueryHandler<Result<UserInfoDto, ErrorList>, GetUserInfoByIdQuery>>();
    }

    [Fact]
    public async Task GetUserInfoById_success_should_return_user_info_with_correct_fields()
    {
        // arrange
        var EMAIL = "test@mail.com";
        var USERNAME = "testUserName";
        var PASSWORD = "Password121314s.";
        
        var user = await DataGenerator.SeedUserAsync(
            USERNAME,
            EMAIL,
            PASSWORD,
            UserManager,
            RoleManager,
            AccountManager);

        var query = new GetUserInfoByIdQuery(user.Id);

        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();

        result.Value.Email.Should().Be(EMAIL);
        result.Value.ParticipantAccount.Should().NotBeNull();
        result.Value.VolunteerAccount.Should().BeNull();
        result.Value.AdminAccount.Should().BeNull();
    }

    [Fact]
    public async Task GetUserInfoById_failure_should_should_return_error_because_of_no_user_with_such_id()
    {
        // arrange
        var EMAIL = "test@mail.com";
        var USERNAME = "testUserName";
        var PASSWORD = "Password121314s.";
        
        var user = await DataGenerator.SeedUserAsync(
            USERNAME,
            EMAIL,
            PASSWORD,
            UserManager,
            RoleManager,
            AccountManager);

        var query = new GetUserInfoByIdQuery(Guid.NewGuid());

        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);

        // assert
        result.IsFailure.Should().BeTrue();
    }
    
    [Fact]
    public async Task GetUserInfoById_success_admin_should_have_only_admin_account()
    {
        // arrange
        var EMAIL = "admin@admin.com";
        
        var user = await UserManager.FindByEmailAsync(EMAIL);

        var query = new GetUserInfoByIdQuery(user.Id);

        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();

        result.Value.Email.Should().Be(EMAIL);
        result.Value.ParticipantAccount.Should().BeNull();
        result.Value.VolunteerAccount.Should().BeNull();
        result.Value.AdminAccount.Should().NotBeNull();
    }
}