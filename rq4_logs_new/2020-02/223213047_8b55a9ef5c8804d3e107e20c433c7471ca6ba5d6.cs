using System;
using NUnit.Framework;
using DFC.App.MatchSkills.Application.Session.Models;
using DFC.App.MatchSkills.Models;
using FluentAssertions;
using DFC.Personalisation.Domain.Models;
using System.Collections.Generic;

namespace DFC.App.MatchSkills.Test.Models
{
    [TestFixture]
    public class SkillSetUnitTests
    {
        [TestFixture]
        public class UniqueSet
        {
            public void When_AddingDuplicateSkill_Then_NoErrorNoDuplicate()
            {
                // Arrange
                var skillset = new SkillSet();
                skillset.Add(new Skill("id1", "name"));
                skillset.Add(new Skill("id2", "name"));
                skillset.Add(new Skill("id2", "name"));
                var sut = new SkillSet();

                // Act
                sut.LoadFrom(skillset);

                // Assert
                sut.Should().HaveCount(2);
            }
        }

        [TestFixture]
        public class LoadFromSession
        {
            [Test]
            public void When_SessionIsNull_Then_SkillSetShouldBeEmpty()
            {
                // Arrange
                UserSession session = null;
                var skillset = new SkillSet();

                // Act
                skillset.LoadFrom(session);

                // Assert
                skillset.Should().BeEmpty();
            }

            [Test]
            public void When_SessionHasSkills_Then_SkillSetShouldHaveSameSkills()
            {
                // Arrange
                UserSession session = new UserSession();
                session.Skills.Add(new UsSkill("id", "name", DateTime.UtcNow));
                var skillset = new SkillSet();

                // Act
                skillset.LoadFrom(session);

                // Assert
                skillset.Should().HaveCount(1);
            }
        }

        [TestFixture]
        public class LoadFromSkillSet
        {
            [Test]
            public void When_SkillSetHasSkills_Then_SkillSetShouldHaveSameSkills()
            {
                // Arrange
                var skillset = new SkillSet();
                skillset.Add(new Skill("id", "name"));
                var sut = new SkillSet();

                // Act
                sut.LoadFrom(skillset);

                // Assert
                sut.Should().HaveCount(1);
            }
        }

        [TestFixture]
        public class LoadFromList
        {
            [Test]
            public void When_ListHasSkills_Then_SkillSetShouldHaveSameSkills()
            {
                // Arrange
                var list = new List<Skill>();
                list.Add(new Skill("id", "name"));
                var sut = new SkillSet();

                // Act
                sut.LoadFrom(list);

                // Assert
                sut.Should().HaveCount(1);
            }
        }
    }
}