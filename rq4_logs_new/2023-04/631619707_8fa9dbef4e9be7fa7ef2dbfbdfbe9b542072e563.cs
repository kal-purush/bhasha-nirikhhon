using AutoMapper;
using NUnit.Framework;
using PocketForzaHorizonCommunity.Back.Database.Entities.Guides;
using PocketForzaHorizonCommunity.Back.DTO.DTOs.GuidesDtos;
using PocketForzaHorizonCommunity.Back.DTO.Mapper;
using PocketForzaHorizonCommunity.Back.DTO.Requests.Guides;

namespace PocketForzaHorizonCommunity.Back.DTO.Tests.MappingProfilesTests;

[TestFixture]
public class TuneMappingTests
{
    [Test]
    public void Tune_To_TuneDto_ShouldMap()
    {
        var tune = Boilerplate.GetTuneSample();
        var expected = MapTuneToDto(tune);
        var mapper = new MapperConfiguration(cfg => cfg.AddProfile<TuneProfile>()).CreateMapper();

        var actual = mapper.Map<Tune, TuneDto>(tune);

        Assert.IsTrue(CompareTunes(expected, actual));
    }

    [Test]
    public void Tune_To_TuneFullInfoDto_Should_Map()
    {
        var tune = Boilerplate.GetTuneSample();
        var expected = MapTuneToFullDto(tune);
        var mapper = new MapperConfiguration(cfg => cfg.AddProfile<TuneProfile>()).CreateMapper();

        var actual = mapper.Map<Tune, TuneFullInfoDto>(tune);

        Assert.IsTrue(CompareTunes(expected, actual));
    }

    [Test]
    public void CreateTuneRequest_To_Tune_Should_Map()
    {
        var request = Boilerplate.GetCreateTuneRequestSample();
        var expected = Boilerplate.GetTuneSample();
        // just created tune should has raiting = 0.0
        expected.Rating = 0.0;

        var mapper = new MapperConfiguration(cfg => cfg.AddProfile<TuneProfile>()).CreateMapper();

        var actual = mapper.Map<CreateTuneRequest, Tune>(request);

        Assert.IsTrue(CompareTunes(expected, actual));
    }

    private TuneDto MapTuneToDto(Tune tune)
    {
        return new TuneDto
        {
            Id = tune.Id.ToString(),
            Title = tune.Title,
            ForzaShareCode = tune.ForzaShareCode,
            Rating = tune.Rating,
            AuthorUserName = tune.User.UserName,
            CarModel = $"{tune.Car.Manufacture.Name} {tune.Car.Model} {tune.Car.Year}",
        };
    }

    private TuneFullInfoDto MapTuneToFullDto(Tune tune)
    {
        return new TuneFullInfoDto
        {
            Id = tune.Id.ToString(),
            Title = tune.Title,
            ForzaShareCode = tune.ForzaShareCode,
            Rating = tune.Rating,
            AuthorUserName = tune.User.UserName,
            CarModel = $"{tune.Car.Manufacture.Name} {tune.Car.Model} {tune.Car.Year}",
            EngineDescription = tune.TuneOptions.EngineDescription,
            Engine = tune.TuneOptions.Engine,
            Aspiration = tune.TuneOptions.Aspiration,
            Intake = tune.TuneOptions.Intake,
            Ignition = tune.TuneOptions.Ignition,
            Displacement = tune.TuneOptions.Displacement,
            Exhaust = tune.TuneOptions.Exhaust,
            HandlingDescription = tune.TuneOptions.HandlingDescription,
            Brakes = tune.TuneOptions.Brakes,
            Suspension = tune.TuneOptions.Suspension,
            AntirollBars = tune.TuneOptions.AntirollBars,
            RollCage = tune.TuneOptions.RollCage,
            DrivetrainDescription = tune.TuneOptions.DrivetrainDescription,
            Clutch = tune.TuneOptions.Clutch,
            Transmission = tune.TuneOptions.Transmission,
            Differential = tune.TuneOptions.Differential,
            TiersDescription = tune.TuneOptions.TiersDescription,
            Compound = tune.TuneOptions.Compound,
            FrontTierWidth = tune.TuneOptions.FrontTierWidth,
            RearTierWidth = tune.TuneOptions.RearTierWidth,
            FrontTrackWidth = tune.TuneOptions.FrontTrackWidth,
            RearTrackWidth = tune.TuneOptions.RearTrackWidth,
        };
    }

    private bool CompareTunes(TuneDto expected, TuneDto actual)
    {
        foreach (var property in actual.GetType().GetProperties())
        {
            if (!property.GetValue(actual).Equals(property.GetValue(expected))) return false;
        }

        return true;
    }

    private bool CompareTunes(TuneFullInfoDto expected, TuneFullInfoDto actual)
    {
        foreach (var property in actual.GetType().GetProperties())
        {
            if (!property.GetValue(actual).Equals(property.GetValue(expected))) return false;
        }

        return true;
    }

    private bool CompareTunes(Tune expected, Tune actual)
    {
        foreach (var property in actual.GetType().GetProperties())
        {
            if (property.Name == nameof(actual.User)) continue;
            if (property.Name == nameof(actual.Car)) continue;
            if (property.Name == nameof(actual.TuneOptions)) continue;
            if (!property.GetValue(actual).Equals(property.GetValue(expected))) return false;
        }

        foreach (var property in actual.TuneOptions.GetType().GetProperties())
        {
            if (property.Name == nameof(actual.TuneOptions.Tune)) continue;
            if (!property.GetValue(actual.TuneOptions).Equals(property.GetValue(expected.TuneOptions))) return false;
        }

        return true;
    }
}