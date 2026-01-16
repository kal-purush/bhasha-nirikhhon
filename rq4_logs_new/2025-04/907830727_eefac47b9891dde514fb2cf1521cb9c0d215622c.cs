using EasyTravel.Application.Factories;
using EasyTravel.Domain.Entites;
using NUnit.Framework;
using System;

[TestFixture]
public class GuideFactoryTests
{
    private GuideFactory _guideFactory;

    [SetUp]
    public void SetUp()
    {
        _guideFactory = new GuideFactory();
    }

    [Test]
    public void CreateInstance_ShouldReturnGuideWithDefaultValues()
    {
        // Act
        var guide = _guideFactory.CreateInstance();

        // Assert
        Assert.That(guide, Is.Not.Null, "Guide instance should not be null.");
        Assert.That(guide.FirstName, Is.EqualTo(string.Empty), "FirstName should be initialized to an empty string.");
        Assert.That(guide.LastName, Is.EqualTo(string.Empty), "LastName should be initialized to an empty string.");
        Assert.That(guide.Email, Is.EqualTo(string.Empty), "Email should be initialized to an empty string.");
        Assert.That(guide.ContactNumber, Is.EqualTo(string.Empty), "ContactNumber should be initialized to an empty string.");
        Assert.That(guide.Address, Is.EqualTo(string.Empty), "Address should be initialized to an empty string.");
        Assert.That(guide.ProfilePicture, Is.EqualTo(string.Empty), "ProfilePicture should be initialized to an empty string.");
        Assert.That(guide.Bio, Is.EqualTo(string.Empty), "Bio should be initialized to an empty string.");
        Assert.That(guide.DateOfBirth, Is.EqualTo(DateTime.MinValue), "DateOfBirth should be initialized to DateTime.MinValue.");
        Assert.That(guide.LanguagesSpoken, Is.EqualTo(string.Empty), "LanguagesSpoken should be initialized to an empty string.");
        Assert.That(guide.PreferredLocations, Is.EqualTo(string.Empty), "PreferredLocations should be initialized to an empty string.");
        Assert.That(guide.PreferredEvents, Is.EqualTo(string.Empty), "PreferredEvents should be initialized to an empty string.");
        Assert.That(guide.LicenseNumber, Is.EqualTo(string.Empty), "LicenseNumber should be initialized to an empty string.");
        Assert.That(guide.Specialization, Is.EqualTo(string.Empty), "Specialization should be initialized to an empty string.");
        Assert.That(guide.YearsOfExperience, Is.EqualTo(0), "YearsOfExperience should be initialized to 0.");
        Assert.That(guide.Availability, Is.False, "Availability should be initialized to false.");
        Assert.That(guide.HourlyRate, Is.EqualTo(0), "HourlyRate should be initialized to 0.");
        Assert.That(guide.Rating, Is.EqualTo(0), "Rating should be initialized to 0.");
        Assert.That(guide.Status, Is.Null, "Status should be initialized to null.");
        Assert.That(guide.AgencyId, Is.Not.EqualTo(Guid.Empty), "AgencyId should be initialized to a new Guid.");
    }
}

