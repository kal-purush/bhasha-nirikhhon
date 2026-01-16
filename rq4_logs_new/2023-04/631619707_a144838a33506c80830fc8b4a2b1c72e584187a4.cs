using PocketForzaHorizonCommunity.Back.Database.Entities;
using PocketForzaHorizonCommunity.Back.Database.Entities.CarEntities;
using PocketForzaHorizonCommunity.Back.Database.Entities.UserStatistics;
using PocketForzaHorizonCommunity.Back.Database.Repos.Interfaces;

namespace PocketForzaHorizonCommunity.Back.Services.Utilities;

/// <summary>
/// As long as Forza Horizon 5 doesn't have any API that allows to get real user statistics information, I need to replace it
/// with some boilerplate data. This class populate all statistics types with random generated data. So, every new user will 
/// get statistics info only through this class and randomness will provide some sort of divercity.
/// </summary>
public class StatisticsGenerator
{
    public ICarRepository CarRepo { get; set; }
    public ApplicationUser User { get; set; }

    private IList<Car> _selectedCars = new List<Car>();

    public StatisticsGenerator(ICarRepository carRepo, ApplicationUser user)
    {
        CarRepo = carRepo;
        User = user;
    }

    public void GenerateStatistics()
    {
        User.OwnedCarsByUsers = GenerateUserCars();
        User.GeneralStatistics = GenerateGeneralStatistics();
        User.CampaignStatistics = GenerateCampaignStatistics();
        User.OnlineStatistics = GenerateOnlineStatistics();
        User.RecordsStatistics = GenerateRecordsStatistics();
    }

    private List<OwnedCarsByUsers> GenerateUserCars()
    {
        var rnd = new Random();
        var cars = CarRepo.GetAll().ToList();

        var _selectedCars = cars.Skip(rnd.Next(0, cars.Count / 2)).Take(rnd.Next(1, cars.Count / 2)).ToList(); ;

        var result = new List<OwnedCarsByUsers>();

        foreach (var car in _selectedCars)
        {
            result.Add(new OwnedCarsByUsers
            {
                CarId = car.Id,
                User = User,
            });
        }

        return result;
    }

    private GeneralStatistics GenerateGeneralStatistics()
    {
        var rnd = new Random();

        var cars = CarRepo.GetAll().ToList();

        var statistics = new GeneralStatistics
        {
            User = User,
            GarageValue = _selectedCars.Sum(c => c.Price),
            TimeDrivenInTicks = rnd.NextInt64(TimeSpan.TicksPerHour, 1000 * TimeSpan.TicksPerHour),
            TotalVictories = rnd.Next(10, 1000),
            TotalCleanLaps = rnd.Next(0, 10_000),
            CollisionsPerRace = rnd.Next(1, 50),
            DailyChallengesCompleted = rnd.Next(1, 1000),
            WeeklyChallengesComplited = rnd.Next(1, 1000),
            FavouriteCarId = _selectedCars[rnd.Next(0, _selectedCars.Count - 1)].Id,
        };

        statistics.TotalPodiums = statistics.TotalVictories + rnd.Next(10, 1000);

        return statistics;
    }

    private CampaignStatistics GenerateCampaignStatistics()
    {

        var rnd = new Random();

        var statistics = new CampaignStatistics
        {
            User = User,
            TotalRaces = rnd.Next(0, 10_000),
            StoriesCompleted = rnd.Next(0, 65),
            StoryStarsEarned = rnd.Next(0, 195),
            HeadToHeadEntered = rnd.Next(0, 1000),
            ExhibitionsCompleted = rnd.Next(0, 84),
            SpeedTrapStars = rnd.Next(0, 93),
            SpeedZoneStars = rnd.Next(0, 78),
            DangerSignStars = rnd.Next(0, 60),
            DriftZoneStars = rnd.Next(0, 60),
            TrailblazerStars = rnd.Next(0, 30),
        };

        statistics.HeadToHeadWon = statistics.HeadToHeadEntered - rnd.Next(0, statistics.HeadToHeadEntered);

        return statistics;
    }

    private OnlineStatistics GenerateOnlineStatistics()
    {
        var rnd = new Random();

        var statistics = new OnlineStatistics
        {
            User = User,
            RecievedKudos = rnd.Next(0, 1000),
            GivenKudos = rnd.Next(0, 1000),
            FlagRushWon = rnd.Next(0, 1000),
            TeamFlagRushWon = rnd.Next(0, 1000),
            FlagsCaptured = rnd.Next(0, 1000),
            InfectedGamesWon = rnd.Next(0, 1000),
            TimesInfectedOthers = rnd.Next(0, 1000),
            TimesInfectedByOthers = rnd.Next(0, 1000),
            TeamKingGamesWon = rnd.Next(0, 1000),
            KingGamesWon = rnd.Next(0, 1000),
            RivalsBeaten = rnd.Next(0, 500),
            RivalsLapsCompleted = rnd.Next(0, 10_000),
        };

        statistics.ArcadeEventsCompleted = statistics.FlagRushWon + statistics.TeamFlagRushWon
            + statistics.InfectedGamesWon + statistics.TeamKingGamesWon + statistics.KingGamesWon;

        statistics.ArcadeEventsEntered = statistics.ArcadeEventsCompleted + rnd.Next(10, 1000);

        return statistics;
    }

    private RecordsStatistics GenerateRecordsStatistics()
    {
        var rnd = new Random();

        var statistics = new RecordsStatistics
        {
            User = User,
            HighestDriftScore = rnd.Next(100_000, 10_000_000),
            HighestDangerSignScore = rnd.NextDouble() * 1200 + 300,
            HighestSpeedTrapScore = rnd.NextDouble() * 500 + 100,
            HighestSpeedZoneScore = rnd.NextDouble() * 500 + 100,
            LongestSkillChainInTicks = rnd.NextInt64(TimeSpan.TicksPerMinute, TimeSpan.TicksPerHour),
            TopSpeed = rnd.NextDouble() * 350 + 250,
            DistanceDriven = rnd.Next(100, 1_000_000),
            LongestDrift = rnd.NextDouble() * 70_000 + 30_000,
        };

        statistics.AvarageSpeed = statistics.TopSpeed - rnd.Next(0, 100);
        statistics.LongestJump = statistics.HighestDangerSignScore;

        return statistics;

    }
}