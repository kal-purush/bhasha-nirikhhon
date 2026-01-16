using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Core.Abstractions;
using PetFamily.IntegrationTests.General;
using PetFamily.IntegrationTests.VolunteerRequests.Heritage;
using PetFamily.VolunteerRequests.Application.Commands.AmendRequest;
using PetFamily.VolunteerRequests.Domain.ValueObjects;

namespace PetFamily.IntegrationTests.VolunteerRequests.HandlerTests;


public class AmendRequest : VolunteerRequestsTestsBase
{
    private readonly ICommandHandler<Guid, AmendRequestCommand> _sut;

    public AmendRequest(VolunteerRequestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider.GetRequiredService<ICommandHandler<Guid, AmendRequestCommand>>();
    }
    
    [Fact]
    public async Task AmendRequest_success_should_set_request_status_to_submitted_and_change_request_info()
    {
        // arrange
        var REVISION_TEXT = "REVISION TEXT";
        var UPDATED_INFO = "UPDATED INFO";
        var adminId = Guid.NewGuid();
        var revisionComment = RevisionComment.Create(REVISION_TEXT).Value;
        
        var seededRequest = await DataGenerator.SeedVolunteerRequest(WriteDbContext);
        seededRequest.SetOnReview(adminId);
        seededRequest.SetRevisionRequired(adminId, revisionComment);
        await WriteDbContext.SaveChangesAsync();
        
        var command = new AmendRequestCommand(seededRequest.Id, seededRequest.UserId, UPDATED_INFO);
        
        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);
        
        // assert
        result.IsSuccess.Should().BeTrue();
        var changedRequest = await WriteDbContext.VolunteerRequests
            .FirstOrDefaultAsync(v => v.Id == seededRequest.Id);
        changedRequest.Should().NotBeNull();
        changedRequest.Status.Value.Should().Be(VolunteerRequestStatusEnum.Submitted);
        
        // changing request by amend should change volunteer info
        changedRequest.VolunteerInfo.Value.Should().Be(UPDATED_INFO);
        
        // should not lose its adminId and revision comment
        changedRequest.RevisionComment.Value.Should().Be(REVISION_TEXT);
        changedRequest.AdminId.Should().Be(adminId);
    }
}