using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TestYourKnowledge.Models;
using TestYourKnowledge.ViewModels;

namespace TestYourKnowledge
{
    internal static class FileHandler
    {
        private const string LeaderboardFilePath = "Leaderboard.csv";
        public static void SavePlayerResult(PlayerResultModel playerResult)
        {
            using (var writer = new StreamWriter(LeaderboardFilePath, true))
            {
                var writeString = $"{playerResult.Name};{playerResult.TimeResult};{playerResult.Score};{playerResult.MaxScore};{playerResult.TimeStart}";
                for (int i = 1; i <= ApplicationViewModel.Instance.StatisticsManager.NumberOfItems; i++)
                {
                    var clickedCount = ApplicationViewModel.Instance.StatisticsManager.SoundStatistics[i].ClickedCount;
                    var isCorrectZeroOne = Convert.ToInt32(ApplicationViewModel.Instance.StatisticsManager.ImageStatistics[i].IsCorrect);
                    writeString += $";{clickedCount};{isCorrectZeroOne}";
                }
                writer.WriteLine(writeString);
            }
        }

        public static List<ResultLeaderboardModel> LoadResults()
        {
            var playerResults = new List<PlayerResultModel>();
            if (File.Exists(LeaderboardFilePath))
            {
                using (var reader = new StreamReader(LeaderboardFilePath))
                {
                    while (!reader.EndOfStream)
                    {
                        var playerResult = reader.ReadLine().Split(';');
                        playerResults.Add(new PlayerResultModel
                        {
                            Name = playerResult[0],
                            TimeResult = int.Parse(playerResult[1]),
                            Score = int.Parse(playerResult[2]),
                            MaxScore = int.Parse(playerResult[3]),
                            TimeStart = DateTime.Parse(playerResult[4])
                        });
                    }

                    var i = 1;
                    return playerResults
                        .OrderByDescending(x => x.Score)
                        .ThenBy(x => x.TimeResult)
                        .Select(x => new ResultLeaderboardModel
                        {
                            No = i++,
                            Name = x.Name,
                            TimeResult = x.TimeResult,
                            Score = x.Score,
                            MaxScore = x.MaxScore,
                            TimeStart = x.TimeStart
                        }).ToList();
                }
            }
            return new List<ResultLeaderboardModel>();
        }
    }
}