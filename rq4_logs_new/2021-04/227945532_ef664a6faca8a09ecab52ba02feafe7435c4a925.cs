using System.Threading.Tasks;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Mithrill.MonsterBook.Domain;

namespace EntityFramework.MonsterBook.Seeds
{
    public static class Creatures
    {
        public static Task AddOrUpdateCreatures(DbContext dbContext)
        {
            return Task.WhenAll(
                AddWolf(dbContext),
                AddHyena(dbContext));
        }

        public static async Task AddWolf(DbContext dbContext)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Wolf",
                    NameHu = "Farkas",
                    Karma = 0,
                    Difficulty = Difficulty.Novice,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    IsUndead = false,
                    AgilityMax = 4,
                    AgilityMin = 3,
                    BodyMax = 3,
                    BodyMin = 3,
                    DexterityMax = 6,
                    DexterityMin = 5,
                    EmotionMax = 1,
                    EmotionMin = 1,
                    Id = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    StrengthMax = 3,
                    StrengthMin = 2,
                    VitalityMax = 5,
                    VitalityMin = 5,
                    WillpowerMax = 2,
                    WillpowerMin = 2
                }
            });

            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 19,
                    SkillLevelMax = 2,
                    SkillLevelMin = 1
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 29,
                    SkillLevelMax = 5,
                    SkillLevelMin = 3
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 31,
                    SkillLevelMax = 5,
                    SkillLevelMin = 3
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 37,
                    SkillLevelMax = 1,
                    SkillLevelMin = 1,
                    GuaranteedSuccesses = 1
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 1,
                    SkillLevelMax = 4,
                    SkillLevelMin = 3
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 12,
                    SkillLevelMax = 2,
                    SkillLevelMin = 1
                }
            });

            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Weapon
                {
                    NameHu = "Harapás",
                    Name = "Bite",
                    Id = 40
                }
            });

            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new CreatureWeapon
                {
                    CreatureId = 1,
                    WeaponId = 40
                }
            });
        }

        public static async Task AddHyena(DbContext dbContext)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = 2,
                    NameHu = "Hiéna",
                    Name = "Hyena",
                    Karma = 0,
                    AgilityMax = 5,
                    AgilityMin = 4,
                    BodyMax = 3,
                    BodyMin = 2,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    DexterityMax = 6,
                    DexterityMin = 5,
                    Difficulty = Difficulty.Novice,
                    EmotionMax = 1,
                    EmotionMin = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 3,
                    StrengthMin = 3,
                    VitalityMax = 5,
                    VitalityMin = 5,
                    WillpowerMax = 1,
                    WillpowerMin = 1
                }
            });

            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 19,
                    SkillLevelMax = 2,
                    SkillLevelMin = 1
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 29,
                    SkillLevelMax = 5,
                    SkillLevelMin = 3
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 31,
                    SkillLevelMax = 5,
                    SkillLevelMin = 3
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 1,
                    SkillLevelMax = 4,
                    SkillLevelMin = 3
                },
                new CreatureSkill
                {
                    CreatureId = 1,
                    SkillId = 12,
                    SkillLevelMax = 2,
                    SkillLevelMin = 1
                }
            });

            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Weapon
                {
                    NameHu = "Harapás",
                    Name = "Bite",
                    Id = 40
                }
            });

            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new CreatureWeapon
                {
                    CreatureId = 1,
                    WeaponId = 40
                }
            });
        }
    }
}