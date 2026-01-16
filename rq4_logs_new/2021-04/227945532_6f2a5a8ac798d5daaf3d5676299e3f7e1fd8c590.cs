using System.Collections.Generic;
using Mithrill.MonsterBook.Domain;

namespace Mithrill.MonsterBook.Application.Common.Adapters
{
    public interface IGeneratedCreature
    {
        int Strength { get; }
        int Vitality { get; }
        int Body { get; }
        int Agility { get; }
        int Dexterity { get; }
        int Intelligence { get; }
        int Willpower { get; }
        int Emotion { get; }
        int DamageReduction { get; }
        int Karma { get; }
        Difficulty Difficulty { get; }
        IEnumerable<IMeritFlaw> Merits { get; }
        IEnumerable<IWeapon> Weapons { get; }
        IEnumerable<ISkill> Skills { get; }
        int PowerPoint { get; }
        int ManaPoint { get; }
        int HitPoint { get; }
    }
}