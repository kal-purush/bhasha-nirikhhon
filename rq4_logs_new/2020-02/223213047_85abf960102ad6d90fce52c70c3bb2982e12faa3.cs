using DFC.App.MatchSkills.Models;
using FluentAssertions;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Models
{
    [TestFixture]
    public class JobProfileTests
    {
        [Test]
        public void When_Constructed_Then_AllMembersShouldBeInitialized()
        {
            // Arrange.

            // Act.
            var sut = new JobProfile();

            // Assert.
            sut.Should().NotBeNull();
            sut.Title.Should().BeEmpty();
            sut.Description.Should().BeEmpty();
            sut.Skills.Should().NotBeNull();
            sut.Skills.Should().HaveCount(0);
        }
    }
}