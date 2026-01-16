using System;
using System.Collections.Generic;
using System.Text;
using DFC.App.MatchSkills.Application.Session.Models;
using DFC.Personalisation.Domain.Models;
using FluentAssertions;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Application.Test.Models
{
    [TestFixture]
    public class Equals
    {
        [Test]
        public void When_SkillsHaveSameId_Then_TheyAreEqual()
        {
            // Arrange
            var id = @"/esco/skill/fe29e658-7cad-4b70-a169-10a240ec0bef";
            var skill_1 = new UsSkill(id, "work in a logistics team", DateTime.UtcNow);
            var skill_2 = new UsSkill(id, "work as part of a logistics team", DateTime.UtcNow);

            // Act
            var isEqual = skill_1.Equals(skill_2);

            // Assert
            isEqual.Should().BeTrue();
        }

        [Test]
        public void When_SkillsHaveDifferentId_Then_TheyAreNotEqual()
        {
            // Arrange
            var id_1 = @"/esco/skill/fe29e658-7cad-4b70-a169-10a240ec0bef";
            var id_2 = @"/esco/skill/fe29e658-7cad-4b70-a169-10a240ec0bff";
            var skill_1 = new UsSkill(id_1, "work in a logistics team", DateTime.UtcNow);
            var skill_2 = new UsSkill(id_2, "work in a logistics team", DateTime.UtcNow);

            // Act
            var isEqual = skill_1.Equals(skill_2);

            // Assert
            isEqual.Should().BeFalse();
        }
    }
}