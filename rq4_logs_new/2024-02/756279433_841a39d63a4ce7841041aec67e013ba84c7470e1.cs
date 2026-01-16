using System.ComponentModel.DataAnnotations;
using AskHand.Domain.Skills.Exceptions;

namespace AskHand.Domain.Skills;

public sealed class Skill : ValueObject
{
    private const int MinLevel = 0;
    private const int MaxLevel = 100;

    [Range(MinLevel, MaxLevel)]
    public int Level { get; }

    private Skill(int level)
    {
        if (level is < MinLevel or > MaxLevel)
        {
            throw new WrongSkillLevelException($"The level choosed is not between {MinLevel} and {MaxLevel} : {level}");
        }

        Level = level;
    }

    public static Skill Create(int level)
    {
        return new Skill(level);
    }

    public override IEnumerable<object> GetEqualityComponents()
    {
        yield return Level;
    }
}