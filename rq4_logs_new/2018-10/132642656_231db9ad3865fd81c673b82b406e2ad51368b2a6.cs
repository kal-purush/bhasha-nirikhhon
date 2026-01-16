using System.Collections.Generic;
using BggCoreSdk.Dto;

namespace BggCoreSdk.UnitTests
{
    public class DataProvider
    {
        public static List<BoardGameDto> GetBoardGameSearchResults()
        {
            return new List<BoardGameDto>()
            {
                new BoardGameDto()
                {
                    Id = "1",
                    Names = new[] { new BoardGameNameDto() { Value = "fake name" } },
                    YearPublished = "1990"
                },
                new BoardGameDto()
                {
                    Id = "2",
                    Names = new[] { new BoardGameNameDto() { Value = "fake name 2" } },
                    YearPublished = "1980"
                }
            };
        }

        public static List<BoardGameDto> GetBoardGameResults()
        {
            return new List<BoardGameDto>()
            {
                new BoardGameDto()
                {
                    Id = "1",
                    YearPublished = "1990",
                    MinPlayers = "5",
                    MaxPlayers = "10",
                    PlayingTime = "15",
                    MinPlayTime = "5",
                    MaxPlayTime = "20",
                    Age = "25",
                    Names = new[]
                    {
                        new BoardGameNameDto { Value = "fake name1", IsPrimary = "true", SortIndex = "1" },
                        new BoardGameNameDto { Value = "fake name2", IsPrimary = "false", SortIndex = "2" }
                    },
                    Description = "this is a description",
                    Thumbnail = "https://path/to/thumbnail.jpeg",
                    Image = "https://path/to/image.jpeg",
                    BoardGameSubdomains = new[]
                    {
                        new ValueIdentifierDto { Id = "1", Value = "value1" },
                        new ValueIdentifierDto { Id = "2", Value = "value2" }
                    },
                    BoardGamePublishers = new[]
                    {
                        new ValueIdentifierDto { Id = "3", Value = "value3" },
                        new ValueIdentifierDto { Id = "4", Value = "value4" }
                    },
                    BoardGameVersions = new[]
                    {
                        new ValueIdentifierDto { Id = "5", Value = "value5" },
                        new ValueIdentifierDto { Id = "6", Value = "value6" }
                    },
                    BoardGameCategories = new[]
                    {
                        new ValueIdentifierDto { Id = "7", Value = "value7" },
                        new ValueIdentifierDto { Id = "8", Value = "value8" }
                    },
                    BoardgameArtists = new[]
                    {
                        new ValueIdentifierDto { Id = "9", Value = "value9" },
                        new ValueIdentifierDto { Id = "10", Value = "value10" }
                    },
                    BoardGameExpansions = new[]
                    {
                        new ValueIdentifierDto { Id = "11", Value = "value11" },
                        new ValueIdentifierDto { Id = "12", Value = "value12" }
                    },
                    BoardGameDesigners = new[]
                    {
                        new ValueIdentifierDto { Id = "13", Value = "value13" },
                        new ValueIdentifierDto { Id = "14", Value = "value14" }
                    },
                    BoardGameFamilies = new[]
                    {
                        new ValueIdentifierDto { Id = "15", Value = "value15" },
                        new ValueIdentifierDto { Id = "16", Value = "value16" }
                    },
                    BoardGameAccessories = new[]
                    {
                        new ValueIdentifierDto { Id = "17", Value = "value17" },
                        new ValueIdentifierDto { Id = "18", Value = "value18" }
                    },
                    VideogameBg = new[]
                    {
                        new ValueIdentifierDto { Id = "19", Value = "value19" },
                        new ValueIdentifierDto { Id = "20", Value = "value20" }
                    },
                    Polls = new[]
                    {
                        new BoardGamePollDto
                        {
                            Name = "suggested_numplayers",
                            Title = "User Suggested Number of Players",
                            TotalVotes = "100",
                            Results = new[]
                            {
                                new BoardGamePollResultDto { Value = "value1", NumVotes = "5" },
                                new BoardGamePollResultDto { Value = "value2", NumVotes = "10" }
                            }
                        }
                    },
                    Comments = new[]
                    {
                        new BoardGameCommentDto
                        {
                            Value = "this is the best game",
                            UserName = "fake user name",
                            Rating = "540"
                        },
                        new BoardGameCommentDto
                        {
                            Value = "this is the worst game",
                            UserName = "another fake user name",
                            Rating = "N/A"
                        }
                    },
                    Statistics = new[]
                    {
                        new BoardGameStatisticsDto
                        {
                            Page = "1",
                            Ratings = new BoardGameStatisticsRatingsDto
                            {
                                UsersRated = "10.22",
                                Average = "5.66",
                                BayesAverage = "8.77",
                                Ranks = new[]
                                {
                                    new BoardGameStatisticsRatingsRankDto
                                    {
                                        Type = "type1",
                                        Id = "1",
                                        Name = "name1",
                                        FriendlyName = "friendlyname1",
                                        Value = "15",
                                        BayesAverage = "5.687"
                                    },
                                    new BoardGameStatisticsRatingsRankDto
                                    {
                                        Type = "type2",
                                        Id = "2",
                                        Name = "name2",
                                        FriendlyName = "friendlyname2",
                                        Value = "20",
                                        BayesAverage = "9.65422"
                                    }
                                },
                                StdDev = "5.6874",
                                Median = "6",
                                Owned = "500",
                                Trading = "20",
                                Wanting = "30",
                                Wishing = "55",
                                NumComments = "99",
                                NumWeights = "2",
                                AverageWeight = "9.612"
                            }
                        }
                    }
                }
            };
        }
    }
}