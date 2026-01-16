using AutoFixture;
using FluentAssertions;
using SFA.DAS.ApprenticeCommitments.Application.Commands.ChangeApprenticeshipCommand;
using SFA.DAS.ApprenticeCommitments.Data.Models;
using System;
using System.Net;
using System.Threading.Tasks;
using TechTalk.SpecFlow;

#nullable enable

namespace SFA.DAS.ApprenticeCommitments.Api.AcceptanceTests.Features
{
    [Binding]
    public class ChangeApprenticeshipSteps
    {
        private readonly Fixture _fixture = new Fixture();
        private readonly TestContext _context;
        private ChangeApprenticeshipCommand _request = null!;
        private CommitmentStatement _commitmentStatement;

        public ChangeApprenticeshipSteps(TestContext context)
        {
            _context = context;
            _commitmentStatement = _fixture.Create<CommitmentStatement>();
        }

        [Given("we have an existing apprenticeship")]
        public async Task GivenWeHaveAnExistingApprenticeship()
        {
            var _apprentice = _fixture.Create<Apprentice>();
            _apprentice.AddApprenticeship(_commitmentStatement);

            _context.DbContext.Apprentices.Add(_apprentice);
            await _context.DbContext.SaveChangesAsync();
        }

        [Given("we have an update apprenticeship request")]
        public void GivenWeHaveAnUpdateApprenticeshipRequest()
        {
            _request = new ChangeApprenticeshipCommand
            {
                ApprenticeshipId = _commitmentStatement.CommitmentsApprenticeshipId,
                Email = "changed@example.com",
                EmployerAccountLegalEntityId = 91111,
                EmployerName = "Changed Employer",
                TrainingProviderId = 92222,
                TrainingProviderName = "Changed provider",
                CourseName = "Changed course",
                CourseLevel = 95555,
                CourseOption = "",
                PlannedStartDate = new DateTime(2091, 03, 20),
                PlannedEndDate = new DateTime(2093, 07, 15),
            };
        }

        [When("the update is posted")]
        public async Task WhenTheUpdateIsPosted()
        {
            await _context.Api.Post("apprenticeships/change", _request);
        }

        [Then("the result should return accepted")]
        public void ThenTheResultShouldReturnAccepted()
        {
            _context.Api.Response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        }

        [Then("the registration exists in database")]
        public void ThenTheRegistrationExistsInDatabase()
        {
            _context.DbContext.CommitmentStatements.Should().ContainEquivalentOf(new
            {
                _commitmentStatement.CommitmentsApprenticeshipId,
                CommitmentsApprovedOn = _request.ApprovedOn,
            });
        }
    }
}