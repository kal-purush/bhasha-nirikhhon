using System.Net;
using Microsoft.AspNetCore.JsonPatch;
using Microsoft.AspNetCore.Mvc;
using SFA.DAS.Recruit.Api.Core;
using SFA.DAS.Recruit.Api.Domain.Entities;
using SFA.DAS.Recruit.Api.Domain.Models;
using SFA.DAS.Recruit.Api.Models;
using SFA.DAS.Recruit.Api.UnitTests;

namespace SFA.DAS.Recruit.Api.IntegrationTests.Controllers.VacancyReviewControllerTests;

public class WhenPatchingVacancyReview: BaseFixture
{
    [Test]
    public async Task Then_The_VacancyReview_Is_NotFound()
    {
        // arrange
        Server.DataContext
            .Setup(x => x.VacancyReviewEntities)
            .ReturnsDbSet(Fixture.CreateMany<VacancyReviewEntity>(10).ToList());

        var patchDocument = new JsonPatchDocument<VacancyReview>();
        patchDocument.Add(x => x.ManualOutcome, "manualOutcome");
        
        // act
        var response = await Client.PatchAsync($"{RouteNames.VacancyReviews}/{Guid.NewGuid()}", patchDocument);

        // assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
    
    [Test]
    public async Task Patching_VacancyReference_Returns_BadRequest()
    {
        // arrange
        var items = Fixture.CreateMany<VacancyReviewEntity>(10).ToList();
        Server.DataContext
            .Setup(x => x.VacancyReviewEntities)
            .ReturnsDbSet(items);

        var patchDocument = new JsonPatchDocument<VacancyReview>();
        patchDocument.Add(x => x.VacancyReference, new VacancyReference(123));
        
        // act
        var response = await Client.PatchAsync($"{RouteNames.VacancyReviews}/{items[1].Id}", patchDocument);
        var errors = await response.Content.ReadAsAsync<ValidationProblemDetails>();


        // assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        errors.Should().NotBeNull();
        errors.Errors.Should().HaveCount(1);
        errors.Errors.Should().ContainKey("/VacancyReference");
    }
    
    [Test]
    public async Task Patching_CreatedDate_Returns_BadRequest()
    {
        // arrange
        var items = Fixture.CreateMany<VacancyReviewEntity>(10).ToList();
        Server.DataContext
            .Setup(x => x.VacancyReviewEntities)
            .ReturnsDbSet(items);

        var patchDocument = new JsonPatchDocument<VacancyReview>();
        patchDocument.Add(x => x.CreatedDate, DateTime.Now);
        
        // act
        var response = await Client.PatchAsync($"{RouteNames.VacancyReviews}/{items[1].Id}", patchDocument);
        var errors = await response.Content.ReadAsAsync<ValidationProblemDetails>();

        // assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        errors.Should().NotBeNull();
        errors.Errors.Should().HaveCount(1);
        errors.Errors.Should().ContainKey("/CreatedDate");
    }
    
    [Test]
    public async Task Then_The_VacancyReview_Is_Patched()
    {
        // arrange - some gymnastics because the mock isn't stateful or a proper DbSet
        var items = Fixture.CreateMany<VacancyReviewEntity>(10).ToList();
        var itemsClone = items.JsonClone();
        var targetItem = itemsClone[4].JsonClone();
        var updatedItem = items[4];
        
        Server.DataContext
            .SetupSequence(x => x.VacancyReviewEntities)
            .ReturnsDbSet(items)
            .ReturnsDbSet(itemsClone);

        var patchDocument = new JsonPatchDocument<VacancyReview>();
        patchDocument.Add(x => x.ManualOutcome, "manualOutcome");
        
        // act
        var response = await Client.PatchAsync($"{RouteNames.VacancyReviews}/{targetItem.Id}", patchDocument);

        // assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        
        Server.DataContext.Verify(x => x.SetValues(ItIs.EquivalentTo(targetItem), ItIs.EquivalentTo(updatedItem)), Times.Once());
        Server.DataContext.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}