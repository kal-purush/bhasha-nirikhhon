using PrizeDrawWeb.Services.Meetup;
using Shouldly;
using System;
using Xunit;

namespace PrizeDrawWebTests
{
    public class MeetupUrlParserTests
    {
        [Theory]
        [InlineData("https://www.meetup.com/dotnetoxford/events/249057796/", "dotnetoxford", "249057796")]
        [InlineData("https://www.meetup.com/dotnet-oxford/events/249057796/", "dotnet-oxford", "249057796")]
        [InlineData("https://www.meetup.com/Milton-Keynes-NET-Meetup-Group/events/250807726/", "Milton-Keynes-NET-Meetup-Group", "250807726")]

        public void MeetupUrlParserTheory(string url, string expectedGroup, string expectedEventId)
        {

            (string group, string eventId) = MeetupUrlParser.Parse(url);
            group.ShouldBe(expectedGroup);
            eventId.ShouldBe(expectedEventId);

        }
    }
}