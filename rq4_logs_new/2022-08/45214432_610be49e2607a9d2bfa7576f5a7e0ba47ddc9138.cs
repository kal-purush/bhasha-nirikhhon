using FluentAssertions;
using MyLfc.Application.Infrastructure;
using MyLfc.Data.Common;
using Xunit;

namespace MyLfc.Application.Tests;
public class MatchPersonHelpersTests
{
    [Theory]
    [InlineData(MatchPersonType.AdditionalReferee, true, MatchPersonType.AdditionalReferee)]
    [InlineData(MatchPersonType.AdditionalReferee, false, MatchPersonType.AdditionalReferee)]
    [InlineData(MatchPersonType.MainReferee, true, MatchPersonType.MainReferee)]
    [InlineData(MatchPersonType.MainReferee, false, MatchPersonType.MainReferee)]
    [InlineData(MatchPersonType.SubReferee, true, MatchPersonType.SubReferee)]
    [InlineData(MatchPersonType.SubReferee, false, MatchPersonType.SubReferee)]
    [InlineData(MatchPersonType.FourthReferee, true, MatchPersonType.FourthReferee)]
    [InlineData(MatchPersonType.FourthReferee, false, MatchPersonType.FourthReferee)]
    [InlineData(MatchPersonType.Team, false, MatchPersonType.Competitor)]
    [InlineData(MatchPersonType.Team, true, MatchPersonType.Team)]
    [InlineData(MatchPersonType.TeamBench, false, MatchPersonType.CompetitorBench)]
    [InlineData(MatchPersonType.TeamBench, true, MatchPersonType.TeamBench)]
    [InlineData(MatchPersonType.Coach, false, MatchPersonType.CompetitorCoach)]
    [InlineData(MatchPersonType.Coach, true, MatchPersonType.Coach)]
    [InlineData(MatchPersonType.Competitor, false, MatchPersonType.Team)]
    [InlineData(MatchPersonType.Competitor, true, MatchPersonType.Competitor)]
    [InlineData(MatchPersonType.CompetitorBench, false, MatchPersonType.TeamBench)]
    [InlineData(MatchPersonType.CompetitorBench, true, MatchPersonType.CompetitorBench)]
    [InlineData(MatchPersonType.CompetitorCoach, false, MatchPersonType.Coach)]
    [InlineData(MatchPersonType.CompetitorCoach, true, MatchPersonType.CompetitorCoach)]
    [InlineData(MatchPersonType.Injury, false, MatchPersonType.InjuryCompetitor)]
    [InlineData(MatchPersonType.Injury, true, MatchPersonType.Injury)]
    [InlineData(MatchPersonType.InjuryCompetitor, false, MatchPersonType.Injury)]
    [InlineData(MatchPersonType.InjuryCompetitor, true, MatchPersonType.InjuryCompetitor)]
    [InlineData(MatchPersonType.Ban, false, MatchPersonType.BanCompetitor)]
    [InlineData(MatchPersonType.Ban, true, MatchPersonType.Ban)]
    [InlineData(MatchPersonType.BanCompetitor, false, MatchPersonType.Ban)]
    [InlineData(MatchPersonType.BanCompetitor, true, MatchPersonType.BanCompetitor)]
    public void VerifyMatchPersonResults(MatchPersonType input, bool isHome, MatchPersonType ouput)
    {
        var result = MatchPersonHelpers.GetMatchPlaceholderType(input, isHome);

        result.Should().Be(ouput);
    }
}