using System.Linq;
using AutoFixture;
using AutoFixture.Kernel;
using AutoFixture.Xunit2;
using AutoMapper;
using FluentAssertions;
using Mithrill.MonsterBook.Application.Common;
using Xunit;

namespace Mithrill.MonsterBook.Application.Tests
{
    public class AutoMapperTests
    {
        private readonly IMapper _mapper;
        private readonly Fixture _fixture;

        public AutoMapperTests()
        {
            var mapperConfiguration = new MapperConfiguration(configure => configure.AddMaps(typeof(Common.Mappings.MappingProfile).Assembly));
            _mapper = new Mapper(mapperConfiguration);

            _fixture = new Fixture();
            _fixture.Behaviors.OfType<ThrowingRecursionBehavior>().ToList()
                .ForEach(b => _fixture.Behaviors.Remove(b));
            _fixture.Behaviors.Add(new OmitOnRecursionBehavior());
            _fixture.Customizations.Add(new TypeRelay(typeof(Common.Adapters.IMeritFlaw), typeof(Domain.Merit)));
            _fixture.Customizations.Add(new TypeRelay(typeof(Common.Adapters.IMeritFlaw), typeof(Domain.Flaw)));
            _fixture.Customizations.Add(new TypeRelay(typeof(Common.Adapters.IWeapon), typeof(Domain.Weapon)));
            _fixture.Customizations.Add(new TypeRelay(typeof(Common.Adapters.IAttackType), typeof(Domain.AttackType)));
            _fixture.Customizations.Add(new TypeRelay(typeof(Common.Adapters.ISkill), typeof(Domain.Skill)));
            _fixture.RepeatCount = 1;
        }

        [Fact]
        public void AssertConfiguration()
        {
            _mapper.ConfigurationProvider.AssertConfigurationIsValid();
        }

        [Fact]
        public void DomainAttackType_To_ApplicationDomainAttackType()
        {
            //Arrange
            var fixture = new Fixture();
            fixture.Behaviors.OfType<ThrowingRecursionBehavior>().ToList()
                .ForEach(b => fixture.Behaviors.Remove(b));
            fixture.Behaviors.Add(new OmitOnRecursionBehavior());
            fixture.RepeatCount = 1;
            var attackType = fixture.Create<MonsterBook.Domain.AttackType>();


            //Act
            var mappedObject = _mapper.Map<Domain.AttackType>(attackType);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Domain.AttackType
            {
                ExtraDamage = attackType.ExtraDamage,
                Name = attackType.Name,
                NumberOfDice = attackType.NumberOfDice
            });
        }

        [Fact]
        public void DomainCreatureSkillCategories_To_ApplicationDomainCreatureSkillCategories()
        {
            //Arrange
            var creatureSkillCategories = _fixture.Create<MonsterBook.Domain.CreatureSkillCategories>();

            //Act
            var mappedObject = _mapper.Map<CreatureSkillCategories>(creatureSkillCategories);

            //Assert
            mappedObject.Should().BeEquivalentTo(new CreatureSkillCategories
            {
                Primary = (SkillCategories)creatureSkillCategories.Primary,
                FirstSecondary = (SkillCategories)creatureSkillCategories.FirstSecondary,
                SecondSecondary = (SkillCategories)creatureSkillCategories.SecondSecondary,
                Tertiary = (SkillCategories)creatureSkillCategories.Tertiary
            });
        }
        
        [Fact]
        public void DomainFlaw_To_ApplicationDomainFlaw()
        {
            //Arrange
            var flaw = _fixture.Create<MonsterBook.Domain.Flaw>();

            //Act
            var mappedObject = _mapper.Map<Domain.Flaw>(flaw);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Domain.Flaw
            {
                Name = flaw.Name,
                NameHu = flaw.NameHu
            });
        }
        
        [Fact]
        public void DomainMerit_To_ApplicationDomainMerit()
        {
            //Arrange
            var merit = _fixture.Create<MonsterBook.Domain.Merit>();

            //Act
            var mappedObject = _mapper.Map<Domain.Merit>(merit);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Domain.Merit
            {
                Name = merit.Name,
                NameHu = merit.NameHu
            });
        }
        
        [Fact]
        public void DomainSkill_To_ApplicationDomainSkill()
        {
            //Arrange
            var skill = _fixture.Create<MonsterBook.Domain.Skill>();

            //Act
            var mappedObject = _mapper.Map<Domain.Skill>(skill);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Domain.Skill
            {
                Name = skill.Name,
                NameHu = skill.NameHu,
                Level = 0,
                Category = (SkillCategories)skill.Category
            });
        }
        
        [Fact]
        public void DomainWeapon_To_ApplicationDomainWeapon()
        {
            //Arrange
            var weapon = _fixture.Create<MonsterBook.Domain.Weapon>();

            //Act
            var mappedObject = _mapper.Map<Domain.Weapon>(weapon);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Domain.Weapon
            {
                Name = weapon.Name,
                NameHu = weapon.NameHu
            });
        }
        
        [Theory, AutoData]
        internal void ApplicationDomainAttackType_To_GeneratedNpcAttackType(Domain.AttackType attackType)
        {
            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpc.AttackType>(attackType);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpc.AttackType
            {
                ExtraDamage = attackType.ExtraDamage,
                Name = attackType.Name,
                NumberOfDice = attackType.NumberOfDice
            });
        }

        [Theory, AutoData]
        internal void ApplicationDomainSkill_To_GeneratedNpcSkill(Domain.Skill skill)
        {
            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpc.Skill>(skill);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpc.Skill
            {
                Name = skill.Name,
                NameHu = skill.NameHu,
                Level = skill.Level,
                Category = skill.Category
            });
        }

        [Fact]
        internal void ApplicationDomainWeapon_To_GeneratedNpcWeapon()
        {
            //Arrange
            var weapon = _fixture.Create<Domain.Weapon>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpc.Weapon>(weapon);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpc.Weapon
            {
                Name = weapon.Name,
                NameHu = weapon.NameHu,
                AttackType = new Npc.Query.GetGeneratedNpc.AttackType
                {
                    ExtraDamage = weapon.AttackType.ExtraDamage,
                    Name = weapon.AttackType.Name,
                    NumberOfDice = weapon.AttackType.NumberOfDice
                }
            });
        }

        [Fact]
        internal void ApplicationDomainGeneratedCreature_To_GeneratedNpc()
        {
            //Arrange
            var creature = _fixture.Create<Domain.GeneratedCreature>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpc.GeneratedNpc>(creature);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpc.GeneratedNpc
            {
                Agility = creature.Agility,
                Body = creature.Body,
                DamageReduction = creature.DamageReduction,
                Dexterity = creature.Dexterity,
                Difficulty = creature.Difficulty,
                Emotion = creature.Emotion,
                HitPoint = creature.HitPoint,
                Intelligence = creature.Intelligence,
                ManaPoint = creature.ManaPoint,
                Strength = creature.Strength,
                Vitality = creature.Vitality,
                Willpower = creature.Willpower,
                Skills = new []
                {
                    new Npc.Query.GetGeneratedNpc.Skill
                    {
                        Name = creature.Skills.First().Name,
                        NameHu = creature.Skills.First().NameHu,
                        Level = creature.Skills.First().Level,
                        Category = creature.Skills.First().Category
                    }
                },
                Weapons = new []
                {
                    new Npc.Query.GetGeneratedNpc.Weapon
                    {
                        Name = creature.Weapons.First().Name,
                        NameHu = creature.Weapons.First().NameHu,
                        AttackType = new Npc.Query.GetGeneratedNpc.AttackType
                        {
                            Name = creature.Weapons.First().AttackType.Name,
                            ExtraDamage = creature.Weapons.First().AttackType.ExtraDamage,
                            NumberOfDice = creature.Weapons.First().AttackType.NumberOfDice
                        } 
                    }
                }
            });
        }

        [Theory, AutoData]
        internal void ApplicationDomainAttackType_To_GeneratedNpcWithKarmaAttackType(Domain.AttackType attackType)
        {
            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpcWithKarma.AttackType>(attackType);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpcWithKarma.AttackType
            {
                ExtraDamage = attackType.ExtraDamage,
                Name = attackType.Name,
                NumberOfDice = attackType.NumberOfDice
            });
        }

        [Theory, AutoData]
        internal void ApplicationDomainSkill_To_GetGeneratedNpcWithKarmaSkill(Domain.Skill skill)
        {
            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpcWithKarma.Skill>(skill);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpcWithKarma.Skill
            {
                Name = skill.Name,
                NameHu = skill.NameHu,
                Level = skill.Level,
                Category = skill.Category
            });
        }

        [Fact]
        internal void ApplicationDomainWeapon_To_GetGeneratedNpcWithKarmaWeapon()
        {
            //Arrange
            var weapon = _fixture.Create<Domain.Weapon>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpcWithKarma.Weapon>(weapon);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpcWithKarma.Weapon
            {
                Name = weapon.Name,
                NameHu = weapon.NameHu,
                AttackType = new Npc.Query.GetGeneratedNpc.AttackType
                {
                    ExtraDamage = weapon.AttackType.ExtraDamage,
                    Name = weapon.AttackType.Name,
                    NumberOfDice = weapon.AttackType.NumberOfDice
                }
            });
        }

        [Fact]
        internal void ApplicationDomainGeneratedCreature_To_GetGeneratedNpcWithKarma()
        {
            //Arrange
            var creature = _fixture.Create<Domain.GeneratedCreature>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedNpcWithKarma.GeneratedNpcWithKarma>(creature);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedNpcWithKarma.GeneratedNpcWithKarma
            {
                Agility = creature.Agility,
                Body = creature.Body,
                DamageReduction = creature.DamageReduction,
                Dexterity = creature.Dexterity,
                Difficulty = creature.Difficulty,
                Emotion = creature.Emotion,
                HitPoint = creature.HitPoint,
                Intelligence = creature.Intelligence,
                ManaPoint = creature.ManaPoint,
                Strength = creature.Strength,
                Vitality = creature.Vitality,
                Willpower = creature.Willpower,
                Skills = new []
                {
                    new Npc.Query.GetGeneratedNpc.Skill
                    {
                        Name = creature.Skills.First().Name,
                        NameHu = creature.Skills.First().NameHu,
                        Level = creature.Skills.First().Level,
                        Category = creature.Skills.First().Category
                    }
                },
                Weapons = new []
                {
                    new Npc.Query.GetGeneratedNpc.Weapon
                    {
                        Name = creature.Weapons.First().Name,
                        NameHu = creature.Weapons.First().NameHu,
                        AttackType = new Npc.Query.GetGeneratedNpc.AttackType
                        {
                            Name = creature.Weapons.First().AttackType.Name,
                            ExtraDamage = creature.Weapons.First().AttackType.ExtraDamage,
                            NumberOfDice = creature.Weapons.First().AttackType.NumberOfDice
                        } 
                    }
                },
                PowerPoint = creature.PowerPoint,
                Karma = creature.Karma
            });
        }

        [Theory, AutoData]
        internal void ApplicationDomainAttackType_To_GeneratedProminentNpcAttackType(Domain.AttackType attackType)
        {
            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedProminentNpc.AttackType>(attackType);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedProminentNpc.AttackType
            {
                ExtraDamage = attackType.ExtraDamage,
                Name = attackType.Name,
                NumberOfDice = attackType.NumberOfDice
            });
        }

        [Theory, AutoData]
        internal void ApplicationDomainSkill_To_GeneratedProminentNpcSkill(Domain.Skill skill)
        {
            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedProminentNpc.Skill>(skill);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedProminentNpc.Skill
            {
                Name = skill.Name,
                NameHu = skill.NameHu,
                Level = skill.Level,
                Category = skill.Category
            });
        }

        [Fact]
        internal void ApplicationDomainWeapon_To_GeneratedProminentNpcWeapon()
        {
            //Arrange
            var weapon = _fixture.Create<Domain.Weapon>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedProminentNpc.Weapon>(weapon);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedProminentNpc.Weapon
            {
                Name = weapon.Name,
                NameHu = weapon.NameHu,
                AttackType = new Npc.Query.GetGeneratedNpc.AttackType
                {
                    ExtraDamage = weapon.AttackType.ExtraDamage,
                    Name = weapon.AttackType.Name,
                    NumberOfDice = weapon.AttackType.NumberOfDice
                }
            });
        }

        [Fact]
        public void ApplicationDomainFlaw_To_GeneratedProminentNpcFlaw()
        {
            //Arrange
            var flaw = _fixture.Create<Domain.Flaw>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedProminentNpc.Flaw>(flaw);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedProminentNpc.Flaw
            {
                Name = flaw.Name,
                NameHu = flaw.NameHu
            });
        }
        
        [Fact]
        public void ApplicationDomainMerit_To_GeneratedProminentNpcMerit()
        {
            //Arrange
            var merit = _fixture.Create<Domain.Merit>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedProminentNpc.Merit>(merit);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedProminentNpc.Merit
            {
                Name = merit.Name,
                NameHu = merit.NameHu
            });
        }

        [Fact]
        internal void ApplicationDomainGeneratedCreature_To_GeneratedProminentNpc()
        {
            //Arrange
            var creature = _fixture.Create<Domain.GeneratedCreature>();

            //Act
            var mappedObject = _mapper.Map<Npc.Query.GetGeneratedProminentNpc.GeneratedProminentNpc>(creature);

            //Assert
            mappedObject.Should().BeEquivalentTo(new Npc.Query.GetGeneratedProminentNpc.GeneratedProminentNpc
            {
                Agility = creature.Agility,
                Body = creature.Body,
                DamageReduction = creature.DamageReduction,
                Dexterity = creature.Dexterity,
                Difficulty = creature.Difficulty,
                Emotion = creature.Emotion,
                HitPoint = creature.HitPoint,
                Intelligence = creature.Intelligence,
                ManaPoint = creature.ManaPoint,
                Strength = creature.Strength,
                Vitality = creature.Vitality,
                Willpower = creature.Willpower,
                Skills = new []
                {
                    new Npc.Query.GetGeneratedProminentNpc.Skill
                    {
                        Name = creature.Skills.First().Name,
                        NameHu = creature.Skills.First().NameHu,
                        Level = creature.Skills.First().Level,
                        Category = creature.Skills.First().Category
                    }
                },
                Weapons = new []
                {
                    new Npc.Query.GetGeneratedProminentNpc.Weapon
                    {
                        Name = creature.Weapons.First().Name,
                        NameHu = creature.Weapons.First().NameHu,
                        AttackType = new Npc.Query.GetGeneratedProminentNpc.AttackType
                        {
                            Name = creature.Weapons.First().AttackType.Name,
                            ExtraDamage = creature.Weapons.First().AttackType.ExtraDamage,
                            NumberOfDice = creature.Weapons.First().AttackType.NumberOfDice
                        } 
                    }
                },
                Flaws = new []
                {
                    new Npc.Query.GetGeneratedProminentNpc.Flaw
                    {
                        Name = creature.Flaws.First().Name,
                        NameHu = creature.Flaws.First().NameHu
                    }
                },
                Merits = new []
                {
                    new Npc.Query.GetGeneratedProminentNpc.Merit
                    {
                        Name = creature.Merits.First().Name,
                        NameHu = creature.Merits.First().NameHu
                    }
                },
                PowerPoint = creature.PowerPoint,
                Karma = creature.Karma
            });
        }
    }
}