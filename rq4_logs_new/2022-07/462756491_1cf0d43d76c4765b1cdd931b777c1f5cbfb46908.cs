using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CthulhuWizard.Application.Requests.Occupations;
using CthulhuWizard.Application.Requests.Occupations.Queries.GetOccupationDetails;
using CthulhuWizard.Persistence.Models;
using CthulhuWizard.Tests.Shared;
using FluentAssertions;
using MediatR.AspNet.Exceptions;
using NUnit.Framework;

namespace CthulhuWizard.Tests.Unit.HandlersTests.Investigators.Queries; 

public class GetOccupationDetailsQueryHandlerTests {
    [Test]
    public async Task Handle_ShouldReturnOccupation() {
        // Arrange
        using var testDb = new RavenTestDb();
        new TestSeeder(testDb).AddOccupations();
        var request = new GetOccupationDetailsQuery();
        var handler = new GetOccupationDetailsQueryHandler(testDb, TestMapper.Instance);
        using var session = testDb.Store.OpenSession();
        var occupations = TestMapper.Instance.Map<List<OccupationDetailsDto>>(session.Query<OccupationEntity>().ToList());
        var expectedOccupation = occupations.First();
        request.Id = Guid.Parse(expectedOccupation.Id!);
        
        // Act
        var result = await handler.Handle(request, CancellationToken.None);
        testDb.WaitForIndexing();
        
        // Assert
        result.Should().BeEquivalentTo(expectedOccupation);

    }
    [Test]
    public async Task Handle__ShouldThrowNotFoundException() {
        // Arrange
        using var testDb = new RavenTestDb();
        new TestSeeder(testDb).AddOccupations();
        var request = new GetOccupationDetailsQuery();
        var handler = new GetOccupationDetailsQueryHandler(testDb, TestMapper.Instance);
        using var session = testDb.Store.OpenSession();
        request.Id = Guid.Empty;
        
        // Act
        var act = () => handler.Handle(request, CancellationToken.None);
        testDb.WaitForIndexing();
        
        // Assert
        await act.Should()
            .ThrowAsync<NotFoundException>()
            .WithMessage($"Occupation with id {request.Id} not found");

    }
}