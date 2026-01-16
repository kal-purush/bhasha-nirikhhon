using AutoFixture;
using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.ApprenticeCommitments.Api.Controllers;
using SFA.DAS.ApprenticeCommitments.Application.Commands.ChangeApprenticeshipCommand;
using SFA.DAS.ApprenticeCommitments.Application.Commands.CreateRegistrationCommand;
using SFA.DAS.ApprenticeCommitments.Application.Commands.VerifyRegistrationCommand;
using SFA.DAS.ApprenticeCommitments.DTOs;
using System.Collections.Generic;
using System.Net;
using System.Net.Mail;
using System.Threading.Tasks;

namespace SFA.DAS.ApprenticeCommitments.Api.AcceptanceTests.Features
{
    public class RegisterRenewConfirm
    {
        private Fixture fixture;

        [SetUp]
        public void Setup()
        {
            fixture = new Fixture();
        }

        [Test]
        public async Task Test()
        {
            var factory = Bindings.Api.CreateApiFactory();
            var client = factory.CreateClient();
            var db = Bindings.Database.CreateDbContext();

            var create = fixture.Build<CreateRegistrationCommand>()
                .With(p => p.Email, (MailAddress adr) => adr.ToString())
                .Create();

            var r1 = await client.PostValueAsync("apprenticeships", create);
            r1.EnsureSuccessStatusCode();

            var verify = fixture.Build<VerifyRegistrationCommand>()
                .With(x => x.ApprenticeId, create.ApprenticeId)
                .With(p => p.Email, create.Email)
                .Create();

            var r2 = await client.PostValueAsync("registrations", verify);
            r2.EnsureSuccessStatusCode();

            var (r3, apprenticeships) = await client.GetValueAsync<List<ApprenticeshipDto>>($"apprentices/{create.ApprenticeId}/apprenticeships");
            var apprenticeshipId = apprenticeships[0].Id;

            var r4 = await client.PostValueAsync(
                $"apprentices/{create.ApprenticeId}/apprenticeships/{apprenticeshipId}/EmployerConfirmation",
                new ConfirmEmployerRequest { EmployerCorrect = true });
            r4.EnsureSuccessStatusCode();

            var change = fixture.Build<ChangeApprenticeshipCommand>()
                .With(x => x.ApprenticeshipId, create.ApprenticeshipId)
                .With(p => p.Email, (MailAddress adr) => adr.ToString())
                .With(p => p.ApprovedOn, (int days) => create.ApprovedOn.AddDays(days))
                .Create();

            var r5 = await client.PostValueAsync("apprenticeships/change", change);
            r5.EnsureSuccessStatusCode();

            (r3, apprenticeships) = await client.GetValueAsync<List<ApprenticeshipDto>>($"apprentices/{create.ApprenticeId}/apprenticeships");
            r3.Should().Be(HttpStatusCode.OK);
            apprenticeships.Should().HaveCount(2);
        }
    }
}