using BuddyDomain.Battle.Entities;
using BuddyDomain.Battle.Repositories;
using BuddyDomain.Battle.Services;
using BuddyDomain.Battle.ValueObjects;
using BuddyDomain.Battle.ValueObjects.Actor;
using BuddyDomain.Battle.ValueObjects.Skill;
using BuddyDomain.Modules;
using BuddyDomain.Repositories;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TestDomain.IntegrationTest
{
    [TestClass]
    public class TestAttackCalculation
    {
        [TestMethod]
        public void TestOnNormalAttack()
        {
            // Create Mock
            var random = new Mock<IRandom>();
            random.Setup(r => r.Random()).Returns(1);
            var actorRepository = new Mock<IActorRepository>();
            var skillRepository = new Mock<ISkillRepository>();

            var attackerId = new ActorId("Attacker");
            var attacker = CreateActor(attackerId);
            var victimId = new ActorId("Victim");
            var victim = CreateActor(victimId);
            var skill = CreateSkill();

            actorRepository.Setup(repo => repo.Find(attackerId))
                .Returns(attacker);
            actorRepository.Setup(repo => repo.Find(victimId))
                .Returns(victim);
            skillRepository.Setup(repo => repo.Find(It.IsAny<SkillId>()))
                .Returns(skill);

            // Create Builders
            var innerBuilder = new AttackPowerBuilderInner();
            var attackPowerBuilder = new AttackPowerBuilder(innerBuilder);
            var defenseBuilder = new DefenseRateBuilder();

            // Create Attack Calculation
            var attackCalculation = new AttackCalculation(
                actorRepository.Object,
                skillRepository.Object,
                attackPowerBuilder,
                defenseBuilder,
                random.Object);

            var damage = attackCalculation.ExecuteCalculation(
                attackerId, victimId, new SkillId("Dummy"));

            // Check Damage
            var damageListener = new Mock<IDamageListener>();
            damageListener.Setup(l => l.ListenDamage(It.IsAny<int>()))
                .Callback<int>(val => Assert.AreEqual(5000, val));
            damage.NotifyDamage(damageListener.Object);
        }

        private static Actor CreateActor(ActorId id)
            => new Actor(
                id,
                new ActorType("Test"),
                new MaxHealthPoint(new HealthPoint(500)),
                new HealthPoint(500),
                new Attack(50),
                new Speed(50),
                new Magic(50));

        private static Skill CreateSkill()
            => new Skill(
                new SkillId("Test"),
                new Target(TargetEnum.OneAlly),
                new Force(100),
                new SkillPoint(10));
    }
}