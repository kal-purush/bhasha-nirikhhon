using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Core.Abstractions;
using PetFamily.Core.Dto.VolunteerRequest;
using PetFamily.Core.Models;
using PetFamily.IntegrationTests.General;
using PetFamily.IntegrationTests.VolunteerRequests.Heritage;
using PetFamily.VolunteerRequests.Application.Queries.GetHandledRequestsByAdminId;
using PetFamily.VolunteerRequests.Domain.ValueObjects;

namespace PetFamily.IntegrationTests.VolunteerRequests.HandlerTests;

public class GetHandledRequestsByAdminIdTests : VolunteerRequestsTestsBase
{
    private readonly IQueryHandler<PagedList<VolunteerRequestDto>, GetHandledRequestsByAdminIdQuery> _sut;

    public GetHandledRequestsByAdminIdTests(VolunteerRequestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider
            .GetRequiredService<IQueryHandler<PagedList<VolunteerRequestDto>, GetHandledRequestsByAdminIdQuery>>();
    }
    
    [Fact]
    public async Task GetHandledRequestsByAdminId_should_return_1_request_because_of_default_filtration()
    {
        // arrange
        var PAGE = 1;
        var PAGE_SIZE = 3;
        
        var adminId1 = Guid.NewGuid();
        var adminId2 = Guid.NewGuid();
        var seededRequest1 = await DataGenerator.SeedVolunteerRequest(WriteDbContext);
        var seededRequest2 = await DataGenerator.SeedVolunteerRequest(WriteDbContext);
        var seededRequest3 = await DataGenerator.SeedVolunteerRequest(WriteDbContext, seededRequest1.UserId);
        
        seededRequest1.SetOnReview(adminId1);
        seededRequest1.SetApproved(adminId1);

        seededRequest2.SetOnReview(adminId2);
        seededRequest2.SetApproved(adminId2);

        seededRequest3.SetOnReview(adminId1);
        await WriteDbContext.SaveChangesAsync();
        
        var query = new GetHandledRequestsByAdminIdQuery(adminId1, PAGE, PAGE_SIZE);
        
        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);
        
        // assert
        result.TotalCount.Should().Be(1);
        result.Items.Count().Should().Be(1);
        result.Items.Should().NotContain(v => v.Id == seededRequest1.Id);
        result.Items.Should().NotContain(v => v.Id == seededRequest2.Id);
        result.Items.Should().Contain(v => v.Id == seededRequest3.Id);
    }
    
    [Fact]
    public async Task GetHandledRequestsByAdminId_should_return_only_2_requests_because_of_filtration()
    {
        // arrange
        var PAGE = 1;
        var PAGE_SIZE = 3;
        
        var adminId1 = Guid.NewGuid();
        var adminId2 = Guid.NewGuid();
        var seededRequest1 = await DataGenerator.SeedVolunteerRequest(WriteDbContext);
        var seededRequest2 = await DataGenerator.SeedVolunteerRequest(WriteDbContext);
        var seededRequest3 = await DataGenerator.SeedVolunteerRequest(WriteDbContext, seededRequest1.UserId);
        var seededRequest4 = await DataGenerator.SeedVolunteerRequest(WriteDbContext);
        
        seededRequest1.SetOnReview(adminId1);
        seededRequest1.SetRejected(adminId1);

        seededRequest2.SetOnReview(adminId2);
        seededRequest2.SetApproved(adminId2);

        seededRequest3.SetOnReview(adminId1);
        seededRequest3.SetRejected(adminId1);
        
        seededRequest4.SetOnReview(adminId1);
        
        await WriteDbContext.SaveChangesAsync();
        
        var query = new GetHandledRequestsByAdminIdQuery(
            adminId1,
            PAGE,
            PAGE_SIZE,
            VolunteerRequestStatusEnum.Rejected.ToString());
        
        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);
        
        // assert
        result.TotalCount.Should().Be(2);
        result.Items.Count().Should().Be(2);
        result.Items.Should().Contain(v => v.Id == seededRequest1.Id);
        result.Items.Should().NotContain(v => v.Id == seededRequest2.Id);
        result.Items.Should().Contain(v => v.Id == seededRequest3.Id);
    }
}