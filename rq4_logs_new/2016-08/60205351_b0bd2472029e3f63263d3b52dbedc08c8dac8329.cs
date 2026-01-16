using System;
using Machine.Specifications;
using Rhino.Mspec.Contrib;

namespace zavit.Domain.Clients.Tests
{
    [Subject("Client")]
    public class ClientTests : TestOf<Client>
    {
        class When_calculating_token_expiry
        {
            Because of = () => _result = Subject.CalculateTokenExpiry(_issueDate);

            It should_add_refresh_token_lifetime_minutes_value_to_the_issue_date = () => _result.ShouldEqual(new DateTime(2015, 3, 2, 1, 20, 59));

            Establish context = () =>
            {
                Subject.RefreshTokenLifeTime = 20;

                _issueDate = new DateTime(2015, 3, 2, 1, 0, 59);
            };

            static DateTime _issueDate;
            static DateTime _result;
        }
    }
}