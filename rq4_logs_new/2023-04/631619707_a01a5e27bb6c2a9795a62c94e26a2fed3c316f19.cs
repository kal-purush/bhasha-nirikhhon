using AutoMapper;
using NUnit.Framework;
using PocketForzaHorizonCommunity.Back.Database.Entities.Guides;
using PocketForzaHorizonCommunity.Back.DTO.DTOs.GuidesDtos;
using PocketForzaHorizonCommunity.Back.DTO.Mapper;
using PocketForzaHorizonCommunity.Back.DTO.Requests.Guides;

namespace PocketForzaHorizonCommunity.Back.DTO.Tests.MappingProfilesTests;

[TestFixture]
public class DesignMappingTests
{
    [Test]
    public void Design_To_DesginDto_Should_Map()
    {
        var design = Boilerplate.GetDesignSample();
        var expected = MapDesignToDto(design);
        var mapper = new MapperConfiguration(cfg => cfg.AddProfile<DesignProfile>()).CreateMapper();

        var actual = mapper.Map<Design, DesignDto>(design);

        Assert.IsTrue(CompareDesigns(expected, actual));
    }

    [Test]
    public void Design_To_DesignFullInfoDto_Should_Map()
    {
        var design = Boilerplate.GetDesignSample();
        var expected = MapToDesignFullInfoDto(design);
        var mapper = new MapperConfiguration(cfg => cfg.AddProfile<DesignProfile>()).CreateMapper();

        var actual = mapper.Map<Design, DesignFullInfoDto>(design);

        Assert.IsTrue(CompareDesigns(expected, actual));
    }

    [Test]
    public void CreateDesignRequest_To_Design_Should_Map()
    {
        var request = Boilerplate.GetCreateDesignRequestSample();
        var expected = Boilerplate.GetDesignSample();

        // for just created designs rating should be 0
        expected.Rating = 0;

        var mapper = new MapperConfiguration(cfg => cfg.AddProfile<DesignProfile>()).CreateMapper();

        var actual = mapper.Map<CreateDesignRequest, Design>(request);

        Assert.IsTrue(CompareDesigns(expected, actual));
    }

    private DesignDto MapDesignToDto(Design design)
    {
        return new DesignDto
        {
            Id = design.Id.ToString(),
            Title = design.Title,
            ForzaShareCode = design.ForzaShareCode,
            Rating = design.Rating,
            AuthorUserName = design.User.UserName,
            CarModel = $"{design.Car.Manufacture.Name} {design.Car.Model} {design.Car.Year}",
        };
    }

    private DesignFullInfoDto MapToDesignFullInfoDto(Design design)
    {
        return new DesignFullInfoDto
        {
            Id = design.Id.ToString(),
            Title = design.Title,
            ForzaShareCode = design.ForzaShareCode,
            Rating = design.Rating,
            AuthorUserName = design.User.UserName,
            CarModel = $"{design.Car.Manufacture.Name} {design.Car.Model} {design.Car.Year}",
            Description = design.DesignOptions.Description,
        };
    }

    private bool CompareDesigns(DesignDto expected, DesignDto actual)
    {
        foreach (var property in actual.GetType().GetProperties())
        {
            if (!property.GetValue(actual).Equals(property.GetValue(expected))) return false;

        }
        return true;
    }

    private bool CompareDesigns(DesignFullInfoDto expected, DesignFullInfoDto actual)
    {
        foreach (var property in actual.GetType().GetProperties())
        {
            if (property.Name == nameof(actual.Images)) continue;
            if (!property.GetValue(actual).Equals(property.GetValue(expected))) return false;

        }
        return true;
    }

    private bool CompareDesigns(Design expected, Design actual)
    {
        foreach (var property in actual.GetType().GetProperties())
        {
            if (property.Name == nameof(actual.Car)) continue;
            if (property.Name == nameof(actual.User)) continue;
            if (property.Name == nameof(actual.DesignOptions)) continue;
            if (!property.GetValue(actual).Equals(property.GetValue(expected))) return false;
        }

        foreach (var property in actual.DesignOptions.GetType().GetProperties())
        {
            if (property.Name == nameof(actual.DesignOptions.Design)) continue;
            if (property.Name == nameof(actual.DesignOptions.PathToImages)) continue;
            if (!property.GetValue(actual.DesignOptions).Equals(property.GetValue(expected.DesignOptions))) return false;
        }

        return true;
    }
}