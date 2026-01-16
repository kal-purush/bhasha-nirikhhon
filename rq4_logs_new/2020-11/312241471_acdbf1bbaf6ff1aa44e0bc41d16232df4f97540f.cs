using BuddyDomain.Battle.Entities;
using BuddyDomain.Battle.Repositories;
using BuddyDomain.Battle.Services;
using BuddyDomain.Battle.ValueObjects.Actor;
using BuddyDomain.Battle.ValueObjects.Skill;
using BuddyDomain.Modules;
using BuddyDomain.Repositories;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TestDomain.Battle.Service
{
    [TestClass]
    public class TestAttackCalculation
    {
        [TestMethod]
        public void TestCalculation()
        {
            // Create Mocks
            var actorRepository = new Mock<IActorRepository>();
            var skillRepository = new Mock<ISkillRepository>();
            var attackPowerBuilder = new Mock<IAttackPowerBuilder>();
            var defenseRateBuilder = new Mock<IDefenseRateBuilder>();
            var damageBuilder = new Mock<IDamageBuilder>();
            var random = new Mock<IRandom>();

            // Create Dummy Id
            var actorId = new ActorId("dummy");
            var skillId = new SkillId("dummy");

            // Create Dummies
            var actor = new Mock<Actor>(
                MockBehavior.Strict,
                actorId,
                new ActorType("dummy"),
                new MaxHealthPoint(new HealthPoint(100)),
                new HealthPoint(100),
                new Attack(100),
                new Speed(100),
                new Magic(100));
            var skill = new Mock<Skill>(
                MockBehavior.Strict,
                skillId,
                new Target(TargetEnum.None),
                new Force(100),
                new SkillPoint(10));

            // Mock Implementation
            actorRepository.Setup(repo => repo.Find(It.IsAny<ActorId>()))
                .Returns(actor.Object);
            skillRepository.Setup(repo => repo.Find(It.IsAny<SkillId>()))
                .Returns(skill.Object);

            var attackCalculation = new AttackCalculation(
                actorRepository.Object,
                skillRepository.Object,
                attackPowerBuilder.Object,
                defenseRateBuilder.Object,
                damageBuilder.Object,
                random.Object);

            attackCalculation.ExecuteCalculation(actorId, actorId, skillId);
        }
    }
}