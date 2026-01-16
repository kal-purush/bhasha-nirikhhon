using FluentAssertions;
using PetFamily.SharedKernel.ValueObjects.Ids;
using PetFamily.VolunteerRequests.Domain.Entities;
using PetFamily.VolunteerRequests.Domain.ValueObjects;

namespace PetFamily.UnitTests;

public class VolunteerRequestsTests
{
    [Fact]
    public void CreateVolunteerRequest_success_should_create_request_with_default_values()
    {
        // Arrange
        var DEFAULT_TEXT = "default text";
        var userId = UserId.NewUserId();
        var volunteerInfo = VolunteerInfo.Create(DEFAULT_TEXT).Value;

        // Act
        var result = new VolunteerRequest(userId, volunteerInfo);

        // Assert
        result.Id.Value.Should().NotBeEmpty();
        result.AdminId.Should().BeNull();
        result.UserId.Should().Be(userId);
        result.VolunteerInfo.Value.Should().Be(DEFAULT_TEXT);
        result.Status.Value.Should().Be(VolunteerRequestStatusEnum.Submitted);
        result.CreatedAt.Should().BeBefore(DateTime.UtcNow);
        result.CreatedAt.Should().BeAfter(DateTime.UtcNow.AddHours(-1));
        result.RejectionComment.Should().BeNull();
    }
    
    [Fact]
    public void CreateVolunteerRequest_success_should_change_status_to_RevisionRequired_and_set_necessary_properties()
    {
        // Arrange
        var DEFAULT_TEXT = "default text";
        var REJECTION_TEXT = "rejection text";
        var userId = UserId.NewUserId();
        var volunteerInfo = VolunteerInfo.Create(DEFAULT_TEXT).Value;
        var request = new VolunteerRequest(userId, volunteerInfo);
        
        var adminId = AdminId.NewAdminId();
        var rejectionComment = RejectionComment.Create(REJECTION_TEXT).Value;

        // Act
        request.SetRevisionRequired(adminId, rejectionComment);

        // Assert
        request.AdminId.Should().Be(adminId);
        request.RejectionComment.Value.Should().Be(rejectionComment.Value);
        request.Status.Value.Should().Be(VolunteerRequestStatusEnum.RevisionRequired);
    }
    
    [Fact]
    public void CreateVolunteerRequest_success_should_change_status_to_OnReview()
    {
        // Arrange
        var DEFAULT_TEXT = "default text";
        var userId = UserId.NewUserId();
        var volunteerInfo = VolunteerInfo.Create(DEFAULT_TEXT).Value;
        var request = new VolunteerRequest(userId, volunteerInfo);
        
        var adminId = AdminId.NewAdminId();

        // Act
        request.SetOnReview(adminId);

        // Assert
        request.AdminId.Should().Be(adminId);
        request.RejectionComment.Should().BeNull();
        request.Status.Value.Should().Be(VolunteerRequestStatusEnum.OnReview);
    }
    
    [Fact]
    public void CreateVolunteerRequest_success_should_change_status_to_Rejected()
    {
        // Arrange
        var DEFAULT_TEXT = "default text";
        var userId = UserId.NewUserId();
        var volunteerInfo = VolunteerInfo.Create(DEFAULT_TEXT).Value;
        var request = new VolunteerRequest(userId, volunteerInfo);
        
        var adminId = AdminId.NewAdminId();

        // Act
        request.SetRejected(adminId);

        // Assert
        request.AdminId.Should().Be(adminId);
        request.RejectionComment.Should().BeNull();
        request.Status.Value.Should().Be(VolunteerRequestStatusEnum.Rejected);
    }
    
    
    [Fact]
    public void CreateVolunteerRequest_success_should_change_status_to_Approved()
    {
        // Arrange
        var DEFAULT_TEXT = "default text";
        var userId = UserId.NewUserId();
        var volunteerInfo = VolunteerInfo.Create(DEFAULT_TEXT).Value;
        var request = new VolunteerRequest(userId, volunteerInfo);
        
        var adminId = AdminId.NewAdminId();

        // Act
        request.SetApproved(adminId);

        // Assert
        request.AdminId.Should().Be(adminId);
        request.RejectionComment.Should().BeNull();
        request.Status.Value.Should().Be(VolunteerRequestStatusEnum.Approved);
    }
}