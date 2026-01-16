using BuddyDomain.Modules;
using BuddyDomain.Repositories;
using BuddyDomain.Repositories.Battle;
using BuddyDomain.ValueObjects.Battle.Actor;
using BuddyDomain.ValueObjects.Battle.Skill;

namespace BuddyDomain.Services.Battle
{
    public class AttackCalculation
    {
        private readonly IRandom random;
        private readonly IActorRepository actorRepository;
        private readonly ISkillRepository skillRepository;
        private readonly AttackPowerBuilder attackPowerBuilder;

        public AttackCalculation(
            IActorRepository actorRepository,
            ISkillRepository skillRepository,
            IRandom random)
        {
            this.actorRepository = actorRepository;
            this.skillRepository = skillRepository;
            this.random = random;
            this.attackPowerBuilder = new AttackPowerBuilder();
        }

        public void ExecuteCalculation(
            ActorId from,
            ActorId to,
            SkillId skillId)
        {
            var attacker = this.actorRepository.Find(from);
            var victim = this.actorRepository.Find(to);
            var skill = this.skillRepository.Find(skillId);

            attacker.NotifyAttackFactor(this.attackPowerBuilder);
            skill.NotifyForceFactor(this.attackPowerBuilder);
            var attackPower = this.attackPowerBuilder.BuildPower();
        }
    }
}