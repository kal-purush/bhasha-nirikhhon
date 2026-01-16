using System.Collections.Generic;
using System.Threading.Tasks;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Mithrill.MonsterBook.Domain;

namespace EntityFramework.MonsterBook.Seeds
{
    public class DragonsAndBugs
    {
        private int _identity;

        public DragonsAndBugs(int identitySeed)
        {
            _identity = identitySeed - 1;

        }

        public async Task<int> AddOrUpdateCreatures(DbContext context)
        {
            await AddBoneDragonAsync(context, GetIdentity());
            await AddBlueDragonAsync(context, GetIdentity());
            await AddRedDragonAsync(context, GetIdentity());
            await AddGreenDragonAsync(context, GetIdentity());
            await AddWorkerBugAsync(context, GetIdentity());
            await AddScoutBugAsync(context, GetIdentity());
            await AddSoldierBugAsync(context, GetIdentity());
            await AddCarverBugAsync(context, GetIdentity());
            await AddQueenBugAsync(context, GetIdentity());

            return _identity;
        }

        private async Task AddQueenBugAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Queen bug",
                    NameHu = "Királynő",
                    Id = identity,
                    StrengthMax = 6,
                    StrengthMin = 4,
                    VitalityMax = 8,
                    VitalityMin = 6,
                    BodyMax = 30,
                    BodyMin = 15,
                    AgilityMax = 10,
                    AgilityMin = 6,
                    DexterityMax = 6,
                    DexterityMin = 4,
                    DamageReductionMax = 8,
                    DamageReductionMin = 3,
                    IntelligenceMax = 8,
                    IntelligenceMin = 8,
                    WillpowerMax = 14,
                    WillpowerMin = 14,
                    Difficulty = Difficulty.Senior,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 6,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 8,
                            SkillLevelMax = 7,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 70,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 10,
                            SkillLevelMax = 5,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 72,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 43
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 52
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 53
                        }
                    },
                    CreatureMerits = new List<CreatureMerit>
                    {
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 75
                        }
                    }
                }
            });
        }

        private async Task AddCarverBugAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Carver bug",
                    NameHu = "Vájó",
                    Id = identity,
                    StrengthMax = 12,
                    StrengthMin = 8,
                    VitalityMax = 4,
                    VitalityMin = 3,
                    BodyMax = 30,
                    BodyMin = 20,
                    AgilityMax = 2,
                    AgilityMin = 1,
                    DexterityMax = 1,
                    DexterityMin = 1,
                    DamageReductionMax = 6,
                    DamageReductionMin = 1,
                    IntelligenceMax = 2,
                    IntelligenceMin = 2,
                    Difficulty = Difficulty.Advanced,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 4,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 43
                        }
                    }
                }
            });
        }

        private async Task AddSoldierBugAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Soldier bug",
                    NameHu = "Katona",
                    Id = identity,
                    StrengthMax = 12,
                    StrengthMin = 6,
                    VitalityMax = 6,
                    VitalityMin = 3,
                    BodyMax = 14,
                    BodyMin = 8,
                    AgilityMax = 6,
                    AgilityMin = 4,
                    DexterityMax = 6,
                    DexterityMin = 4,
                    DamageReductionMax = 14,
                    DamageReductionMin = 4,
                    IntelligenceMax = 2,
                    IntelligenceMin = 2,
                    Difficulty = Difficulty.Senior,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 7,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 70,
                            SkillLevelMax = 4,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 43
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 52
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 53
                        }
                    }
                }
            });
        }

        private async Task AddScoutBugAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Scout bug",
                    NameHu = "Felderítő",
                    Id = identity,
                    StrengthMax = 4,
                    StrengthMin = 2,
                    VitalityMax = 6,
                    VitalityMin = 3,
                    BodyMax = 6,
                    BodyMin = 3,
                    AgilityMax = 10,
                    AgilityMin = 5,
                    DexterityMax = 6,
                    DexterityMin = 4,
                    IntelligenceMax = 4,
                    IntelligenceMin = 4,
                    DamageReductionMax = 6,
                    DamageReductionMin = 1,
                    Difficulty = Difficulty.Experienced,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 14,
                            SkillLevelMax = 4,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 8,
                            SkillLevelMax = 7,
                            SkillLevelMin = 3,
                            GuaranteedSuccesses = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 5,
                            SkillLevelMin = 1,
                            GuaranteedSuccesses = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 70,
                            SkillLevelMax = 4,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 43
                        }
                    }
                }
            });
        }

        private async Task AddWorkerBugAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Worker bug",
                    NameHu = "Dolgozó",
                    Id = identity,
                    StrengthMax = 10,
                    StrengthMin = 5,
                    VitalityMax = 6,
                    VitalityMin = 1,
                    BodyMax = 8,
                    BodyMin = 4,
                    AgilityMax = 3,
                    AgilityMin = 1,
                    DexterityMax = 2,
                    DexterityMin = 1,
                    DamageReductionMax = 6,
                    DamageReductionMin = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    Difficulty = Difficulty.Experienced,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 3,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 43
                        }
                    }
                }
            });
        }

        private async Task AddGreenDragonAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Green dragon",
                    NameHu = "Zöld sárkány",
                    Id = identity,
                    StrengthMax = 14,
                    StrengthMin = 6,
                    VitalityMax = 10,
                    VitalityMin = 6,
                    BodyMax = 20,
                    BodyMin = 8,
                    AgilityMax = 6,
                    AgilityMin = 3,
                    DexterityMax = 6,
                    DexterityMin = 3,
                    IntelligenceMax = 8,
                    IntelligenceMin = 4,
                    WillpowerMax = 8,
                    WillpowerMin = 4,
                    EmotionMax = 8,
                    EmotionMin = 4,
                    DamageReductionMax = 8,
                    DamageReductionMin = 4,
                    Difficulty = Difficulty.Senior,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 70,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 49,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            WeaponId = 43,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 40,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 50,
                            CreatureId = identity
                        }
                    }
                }
            });
        }

        private async Task AddRedDragonAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Red dragon",
                    NameHu = "Vörös sárkány",
                    Id = identity,
                    StrengthMax = 30,
                    StrengthMin = 8,
                    VitalityMax = 10,
                    VitalityMin = 6,
                    BodyMax = 40,
                    BodyMin = 8,
                    AgilityMax = 6,
                    AgilityMin = 3,
                    DexterityMax = 6,
                    DexterityMin = 3,
                    IntelligenceMax = 8,
                    IntelligenceMin = 4,
                    WillpowerMax = 8,
                    WillpowerMin = 4,
                    EmotionMax = 6,
                    EmotionMin = 2,
                    DamageReductionMax = 14,
                    DamageReductionMin = 8,
                    Difficulty = Difficulty.Expert,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 70,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 49,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 64,
                            SkillLevelMax = 9,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            WeaponId = 43,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 40,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 51,
                            CreatureId = identity
                        }
                    }
                }
            });
        }

        private async Task AddBlueDragonAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Blue dragon",
                    NameHu = "Kék sárkány",
                    Id = identity,
                    StrengthMax = 24,
                    StrengthMin = 6,
                    VitalityMax = 10,
                    VitalityMin = 6,
                    BodyMax = 30,
                    BodyMin = 8,
                    AgilityMax = 6,
                    AgilityMin = 4,
                    DexterityMax = 6,
                    DexterityMin = 4,
                    IntelligenceMax = 8,
                    IntelligenceMin = 4,
                    WillpowerMax = 6,
                    WillpowerMin = 4,
                    EmotionMax = 8,
                    EmotionMin = 2,
                    DamageReductionMax = 12,
                    DamageReductionMin = 6,
                    Difficulty = Difficulty.Expert,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 70,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 49,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 65,
                            SkillLevelMax = 9,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureFlaws = new List<CreatureFlaw>
                    {
                        new CreatureFlaw
                        {
                            CreatureId = identity,
                            FlawId = 42
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            WeaponId = 43,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 40,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 50,
                            CreatureId = identity
                        }
                    }
                }
            });
        }

        private async Task AddBoneDragonAsync(DbContext context, int identity)
        {
            await context.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Bone dragon",
                    NameHu = "Csontsárkány",
                    Id = identity,
                    StrengthMax = 32,
                    StrengthMin = 16,
                    BodyMax = 20,
                    BodyMin = 8,
                    AgilityMax = 3,
                    AgilityMin = 3,
                    DexterityMax = 3,
                    DexterityMin = 3,
                    IntelligenceMax = 8,
                    IntelligenceMin = 4,
                    WillpowerMax = 8,
                    WillpowerMin = 4,
                    EmotionMax = 8,
                    EmotionMin = 4,
                    KarmaMax = 4,
                    KarmaMin = 7,
                    DamageReductionMax = 10,
                    DamageReductionMin = 10,
                    IsUndead = true,
                    Difficulty = Difficulty.Expert,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 15,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 70,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 49,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 68,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureMerits = new List<CreatureMerit>
                    {
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 87
                        },
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 88
                        },
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 89
                        },
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 90
                        },
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 91
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            WeaponId = 43,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 40,
                            CreatureId = identity
                        },
                        new CreatureWeapon
                        {
                            WeaponId = 49,
                            CreatureId = identity
                        }
                    }
                }
            });
        }

        private int GetIdentity()
        {
            return _identity++;
        }
    }
}