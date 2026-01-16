using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using FluentAssertions;
using FluentAssertions.Execution;
using Microsoft.EntityFrameworkCore;
using Mithrill.MonsterBook.Application.Common.Adapters;
using Mithrill.MonsterBook.Application.Common.Builders;
using Mithrill.MonsterBook.Domain;
using Xunit;
using Attribute = Mithrill.MonsterBook.Domain.Attribute;
using Difficulty = Mithrill.MonsterBook.Application.Common.Difficulty;

namespace Mithrill.MonsterBook.Application.Tests
{
    public class CreatureBuilderTests
    {
        private readonly IMonsterBookDbContext _monsterBookDbContext;
        private readonly INpcBuilder<IGeneratedCreature> _creatureBuilder;
        private static readonly CancellationToken CancellationToken = CancellationToken.None;

        public CreatureBuilderTests()
        {
            var mapperConfiguration = new MapperConfiguration(configure => configure.AddMaps(typeof(Common.Mappings.MappingProfile).Assembly));
            var mapper = new Mapper(mapperConfiguration);
            _monsterBookDbContext = new TestDbContext(
                new DbContextOptionsBuilder()
                    .UseInMemoryDatabase(Guid.NewGuid().ToString())
                    .Options);
            _creatureBuilder = new CreatureBuilder(mapper, _monsterBookDbContext);
        }

        #region Difficulty
        [Fact]
        public async Task Given_SetDefaultStatsIsCalledWithLowerDifficultyThenInDatabase_Then_DifficultyShouldBeEqualToWhatIsInTheDatabase()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats(Difficulty.Newbie);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Difficulty.Should().BeEquivalentTo(creature.Difficulty);
        }

        [Fact]
        public async Task Given_SetDefaultStatsIsCalledWithHigherDifficultyThenInDatabase_Then_DifficultyShouldBeEqualToArgument()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats(Difficulty.Expert);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Difficulty.Should().BeEquivalentTo(Difficulty.Expert);
        }

        [Fact]
        public async Task Given_SetDefaultStatsIsCalledWithOutDifficulty_Then_DifficultyShouldBeEqualToWhatItIsInDatabase()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Difficulty.Should().BeEquivalentTo(creature.Difficulty);
        }
        #endregion

        #region Stats
        [Fact]
        public async Task Given_SetDefaultStatsCalledButMinMaxStatsAreBackWardsForAgility_Then_ShouldThrowArgumentOutOfRangeException()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.AgilityMax = 4;
            creature.AgilityMin = 8;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            Action action = () => _creatureBuilder.SetDefaultStats();

            //Assert
            action.Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public async Task Given_SetDefaultStatsIsCalledButMinMaxStatsAreBackWardsForAgility_Then_ShouldThrowArgumentOutOfRangeException()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.AgilityMax = 4;
            creature.AgilityMin = 8;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            Action action = () => _creatureBuilder.SetDefaultStats();

            //Assert
            action.Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public async Task Given_SetRacialStatsIsCalledForLivingCreature_Then_ShouldNotIncreaseStrengthOrBody()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddRacialModifiers(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            using var assertionScope = new AssertionScope();
            generatedCreature.Strength.Should().Be(0);
            generatedCreature.Body.Should().Be(0);
        }

        [Fact]
        public async Task Given_SetRacialStatsIsCalledForUndeadCreatureThatIsStoredAsLiving_Then_ShouldIncreaseStrengthOrBody()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddRacialModifiers(true);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            using var assertionScope = new AssertionScope();
            generatedCreature.Strength.Should().Be(2);
            generatedCreature.Body.Should().Be(2);
        }

        [Fact]
        public async Task Given_SetRacialStatsIsCalledForUndeadCreatureThatIsStoredAsUndead_Then_ShouldNotIncreaseStrengthOrBody()
        {
            //Arrange
            var creature = new Seeds().UndeadCreature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddRacialModifiers(true);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            using var assertionScope = new AssertionScope();
            generatedCreature.Strength.Should().Be(0);
            generatedCreature.Body.Should().Be(0);
        }
        #endregion

        #region Karma
        [Fact]
        public async Task Given_SetKarmaIsCalledForNewbieDifficultyForCreature_Then_KarmaShouldBeEqualToCreaturesBaseKarma()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.GenerateKarma(false, Difficulty.Newbie);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Karma.Should().Be(creature.Karma);
        }

        [Fact]
        public async Task Given_SetKarmaIsCalledForSeasonedDifficultyForCreatureWithNegativeKarma_Then_KarmaShouldBeNegativeAndDecreasedByTwoRelativeToCreaturesBaseKarma()
        {
            //Arrange
            var creature = new Seeds().UndeadCreature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.GenerateKarma(true, Difficulty.Seasoned);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Karma.Should().Be(creature.Karma-2);
        }

        [Fact]
        public async Task Given_SetKarmaIsCalledForSeasonedDifficultyForEvilCreatureWithNoKarma_Then_KarmaShouldBeNegativeAndDecreasedByTwoRelativeToCreaturesBaseKarma()
        {
            //Arrange
            var creature = new Seeds().UndeadCreature;
            creature.Karma = 0;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.GenerateKarma(true, Difficulty.Seasoned);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Karma.Should().Be(-2);
        }

        [Fact]
        public async Task Given_SetKarmaIsCalledForSeasonedDifficultyForGoodCreatureWithNoKarma_Then_KarmaShouldBePositiveAndIncreasedByTwoRelativeToCreaturesBaseKarma()
        {
            //Arrange
            var creature = new Seeds().UndeadCreature;
            creature.Karma = 0;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.GenerateKarma(false, Difficulty.Seasoned);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Karma.Should().Be(2);
        }

        #endregion

        #region LifeSigns
        #region PowerPoint
        [Fact]
        public async Task Given_CalculateLifeSignsCalledForEvilCreature_Then_PowerPointsShouldBePositiveAndThreeTimesKarma()
        {
            //Arrange
            var creature = new Seeds().UndeadCreature;
            creature.Karma = -3;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.GenerateKarma(true);
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.PowerPoint.Should().Be(9);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForGoodCreature_Then_PowerPointsShouldCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.Karma = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.GenerateKarma(false);
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.PowerPoint.Should().Be(12);
        }
        #endregion

        #region ManaPoint
        [Fact]
        public async Task Given_CalculateLifeSignsCalledForCreatureBelowEightIntelligenceWithoutMerits_Then_ManaShouldBeCalculatedNormally()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.IntelligenceMax = 4;
            creature.IntelligenceMin = 4;
            creature.WillpowerMax = 4;
            creature.WillpowerMin = 4;
            creature.EmotionMax = 4;
            creature.EmotionMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.ManaPoint.Should().Be(12);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForCreatureEightIntelligenceWithoutMerits_Then_IncreasedManaShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.IntelligenceMax = 8;
            creature.IntelligenceMin = 8;
            creature.WillpowerMax = 4;
            creature.WillpowerMin = 4;
            creature.EmotionMax = 4;
            creature.EmotionMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.ManaPoint.Should().Be(24);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForCreatureEightIntelligenceWithMerits_Then_IncreasedManaShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.IntelligenceMax = 8;
            creature.IntelligenceMin = 8;
            creature.WillpowerMax = 4;
            creature.WillpowerMin = 4;
            creature.EmotionMax = 4;
            creature.EmotionMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.AddMerits(Difficulty.Proficient);
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.ManaPoint.Should().Be(35);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForCreatureBelowEightIntelligenceWithMerits_Then_IncreasedManaShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.IntelligenceMax = 4;
            creature.IntelligenceMin = 4;
            creature.WillpowerMax = 4;
            creature.WillpowerMin = 4;
            creature.EmotionMax = 4;
            creature.EmotionMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.AddMerits(Difficulty.Proficient);
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.ManaPoint.Should().Be(19);
        }
        #endregion

        #region HitPoints
        [Fact]
        public async Task Given_CalculateLifeSignsCalledForLivingCreatureBelowEightBodyWithoutMerits_Then_HpShouldBeCalculatedNormally()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.BodyMax = 4;
            creature.BodyMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(21);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForLivingCreatureBelowEightBodyWithMerits_Then_HpShouldBeCalculatedNormally()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.BodyMax = 4;
            creature.BodyMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.AddMerits(Difficulty.Proficient);
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(25);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForLivingCreatureWithEightBodyWithMerits_Then_IncreasedHpShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.BodyMax = 8;
            creature.BodyMin = 8;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.AddMerits(Difficulty.Proficient);
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(90);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForLivingCreatureWithEightBodyWithoutMerits_Then_IncreasedHpShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.BodyMax = 8;
            creature.BodyMin = 8;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.CalculateLifeSigns(false);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(74);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForUndeadCreatureBelowEightBodyWithoutMerits_Then_UndeadHpShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.StrengthMax = 4;
            creature.StrengthMin = 4;
            creature.BodyMax = 4;
            creature.BodyMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.CalculateLifeSigns(true);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(40);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForUndeadCreatureBelowEightBodyWithMerits_Then_UndeadHpShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.StrengthMax = 4;
            creature.StrengthMin = 4;
            creature.BodyMax = 4;
            creature.BodyMin = 4;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.AddMerits(Difficulty.Proficient);
            _creatureBuilder.CalculateLifeSigns(true);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(40);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForUndeadCreatureWithEightBodyWithMerits_Then_UndeadHpShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.StrengthMax = 8;
            creature.StrengthMin = 8;
            creature.BodyMax = 8;
            creature.BodyMin = 8;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.AddMerits(Difficulty.Proficient);
            _creatureBuilder.CalculateLifeSigns(true);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(80);
        }

        [Fact]
        public async Task Given_CalculateLifeSignsCalledForUndeadCreatureWithEightBodyWithoutMerits_Then_UndeadHpShouldBeCalculated()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.StrengthMax = 8;
            creature.StrengthMin = 8;
            creature.BodyMax = 8;
            creature.BodyMin = 8;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.SetDefaultStats();
            _creatureBuilder.CalculateLifeSigns(true);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.HitPoint.Should().Be(80);
        }
        #endregion
        #endregion

        #region Skill
        [Fact]
        public async Task Given_AddSkillsCalledDifficultyNotSpecified_Then_SkillLevelShouldComeFromTheDatabase()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.CreatureSkills = new List<CreatureSkill>
            {
                new CreatureSkill
                {
                    CreatureId = creature.Id,
                    Creature = creature,
                    SkillId = 1,
                    SkillLevelMax = 3,
                    SkillLevelMin = 3,
                    GuaranteedSuccesses = 1,
                    Skill = new Skill
                    {
                        Id = 1,
                        Name = "Skill",
                        NameHu = "Skill",
                        Attribute1 = Attribute.Dexterity,
                        Attribute2 = Attribute.Strength,
                        Category = SkillCategories.Combat
                    }
                }
            };
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddSkills();
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Skills.Should().BeEquivalentTo(new Domain.Skill
            {
                Name = creature.CreatureSkills.First().Skill.Name,
                NameHu = creature.CreatureSkills.First().Skill.NameHu,
                Level = 3,
                GuaranteedSuccesses = creature.CreatureSkills.First().GuaranteedSuccesses,
                Category = (Common.SkillCategories)creature.CreatureSkills.First().Skill.Category
            });
        }

        [Fact]
        public async Task Given_AddSkillsCalledDifficultySkillful_Then_SkillLevelShouldBeIncreasedByTwo()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.CreatureSkills = new List<CreatureSkill>
            {
                new CreatureSkill
                {
                    CreatureId = creature.Id,
                    Creature = creature,
                    SkillId = 1,
                    SkillLevelMax = 3,
                    SkillLevelMin = 3,
                    GuaranteedSuccesses = 1,
                    Skill = new Skill
                    {
                        Id = 1,
                        Name = "Skill",
                        NameHu = "Skill",
                        Attribute1 = Attribute.Dexterity,
                        Attribute2 = Attribute.Strength,
                        Category = SkillCategories.Combat
                    }
                }
            };
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddSkills(Difficulty.Skillful);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Skills.Should().BeEquivalentTo(new Domain.Skill
            {
                Name = creature.CreatureSkills.First().Skill.Name,
                NameHu = creature.CreatureSkills.First().Skill.NameHu,
                Level = 5,
                GuaranteedSuccesses = creature.CreatureSkills.First().GuaranteedSuccesses,
                Category = (Common.SkillCategories)creature.CreatureSkills.First().Skill.Category
            });
        }
        #endregion

        #region Merits
        [Fact]
        public async Task Given_AddMeritsCalledDifficultyNotSpecified_Then_AllMeritsRegisteredToTheCreatureShouldBeAdded()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddMerits();
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Merits.Should().BeEquivalentTo(new []
            {
                new Domain.Merit
                {
                    Name = creature.CreatureMerits.First().Merit.Name,
                    NameHu = creature.CreatureMerits.First().Merit.NameHu
                },
                new Domain.Merit
                {
                    Name = creature.CreatureMerits.Last().Merit.Name,
                    NameHu = creature.CreatureMerits.Last().Merit.NameHu
                }
            });
        }

        [Fact]
        public async Task Given_AddMeritsCalledDifficultySetToNewbie_Then_OneMeritRegisteredToTheCreatureShouldBeAdded()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddMerits(Difficulty.Newbie);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Merits.Count().Should().Be(1);
        }

        [Fact]
        public async Task Given_AddMeritsCalledDifficultySetToSenior_Then_TwoMeritShouldBeAddedBecauseTheCreatureOnlyHaveTwoInDatabase()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddMerits(Difficulty.Senior);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Merits.Count().Should().Be(2);
            generatedCreature.Merits.Should().BeEquivalentTo(new []
            {
                new Domain.Merit
                {
                    Name = creature.CreatureMerits.First().Merit.Name,
                    NameHu = creature.CreatureMerits.First().Merit.NameHu
                },
                new Domain.Merit
                {
                    Name = creature.CreatureMerits.Last().Merit.Name,
                    NameHu = creature.CreatureMerits.Last().Merit.NameHu
                }
            });
        }
        #endregion

        #region Flaws

        private static List<CreatureFlaw> GetFlaws(int creatureId)
        {
            return new List<CreatureFlaw>
            {
                new CreatureFlaw
                {
                    CreatureId = creatureId,
                    FlawId = 1,
                    Flaw = new Flaw
                    {
                        Id = 1,
                        Name = "Flaw",
                        NameHu = "Flaw"
                    }
                },
                new CreatureFlaw
                {
                    CreatureId = creatureId,
                    FlawId = 2,
                    Flaw = new Flaw
                    {
                        Id = 2,
                        Name = "Flaw",
                        NameHu = "Flaw"
                    }
                },
                new CreatureFlaw
                {
                    CreatureId = creatureId,
                    FlawId = 3,
                    Flaw = new Flaw
                    {
                        Id = 3,
                        Name = "Flaw",
                        NameHu = "Flaw"
                    }
                },
                new CreatureFlaw
                {
                    CreatureId = creatureId,
                    FlawId = 4,
                    Flaw = new Flaw
                    {
                        Id = 4,
                        Name = "Flaw",
                        NameHu = "Flaw"
                    }
                },
                new CreatureFlaw
                {
                    CreatureId = creatureId,
                    FlawId = 5,
                    Flaw = new Flaw
                    {
                        Id = 5,
                        Name = "Flaw",
                        NameHu = "Flaw"
                    }
                },
                new CreatureFlaw
                {
                    CreatureId = creatureId,
                    FlawId = 6,
                    Flaw = new Flaw
                    {
                        Id = 6,
                        Name = "Flaw",
                        NameHu = "Flaw"
                    }
                },
                new CreatureFlaw
                {
                    CreatureId = creatureId,
                    FlawId = 7,
                    Flaw = new Flaw
                    {
                        Id = 7,
                        Name = "Flaw",
                        NameHu = "Flaw"
                    }
                }
            };
        }

        [Fact]
        public async Task Given_AddFlawsCalledDifficultyNotSpecified_Then_AllFlawsRegisteredToTheCreatureShouldBeAdded()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.CreatureFlaws = GetFlaws(creature.Id);
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddFlaws();
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Flaws.Count().Should().Be(7);
        }

        [Fact]
        public async Task Given_AddFlawsCalledDifficultySetToSkilled_Then_SevenFlawsShouldBeAddedToTheCreature()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.CreatureFlaws = GetFlaws(creature.Id);
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddFlaws(Difficulty.Skilled);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Flaws.Count().Should().Be(5);
        }

        [Fact]
        public async Task Given_AddFlawsCalledDifficultySetToSenior_Then_TwoFlawsShouldBeAddedToTheCreature()
        {
            //Arrange
            var creature = new Seeds().Creature;
            creature.CreatureFlaws = GetFlaws(creature.Id);
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddFlaws(Difficulty.Senior);
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Flaws.Count().Should().Be(2);
        }

        [Fact]
        public async Task Given_AddFlawsCalledWithOutDifficultySet_Then_OneFlawsShouldBeAddedToTheCreatureBecauseOnlyOneIsRegistredToItInTheDatabase()
        {
            //Arrange
            var creature = new Seeds().Creature;
            await _monsterBookDbContext.Creatures.AddAsync(creature);
            await _monsterBookDbContext.SaveChangesAsync(CancellationToken);

            //Act
            await _creatureBuilder.GetMonsterFromDatabaseAsync(1, CancellationToken);
            _creatureBuilder.AddFlaws();
            var generatedCreature = _creatureBuilder.GetNpc();

            //Assert
            generatedCreature.Flaws.Count().Should().Be(1);
        }
        #endregion
    }
}