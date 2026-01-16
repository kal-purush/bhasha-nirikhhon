using System.Collections.Generic;
using System.Threading.Tasks;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Mithrill.MonsterBook.Domain;

namespace EntityFramework.MonsterBook.Seeds
{
    public class Animals
    {
        private static int _identity;

        public Animals(int identitySeed)
        {
            _identity = identitySeed;
        }

        public async Task<int> AddOrUpdateAnimals(DbContext dbContext)
        {
            await AddWolf(dbContext, GetIdentity());
            await AddHyena(dbContext, GetIdentity());
            await AddVenomousSnake(dbContext, GetIdentity());
            await AddNonVenomousSnake(dbContext, GetIdentity());
            await AddCrocodile(dbContext, GetIdentity());
            await AddDog(dbContext, GetIdentity());
            await AddHeavyHorse(dbContext, GetIdentity());
            await AddLightHorse(dbContext, GetIdentity());
            await AddCat(dbContext, GetIdentity());
            await AddBear(dbContext, GetIdentity());
            await AddLion(dbContext, GetIdentity());
            await AddSpider(dbContext, GetIdentity());
            await AddReu(dbContext, GetIdentity());
            await AddScorpion(dbContext, GetIdentity());
            await AddTiger(dbContext, GetIdentity());
            await AddFalcon(dbContext, GetIdentity());
            await AddWarg(dbContext, GetIdentity());
            return _identity;
        }

        public async Task AddWolf(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Name = "Wolf",
                    NameHu = "Farkas",
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
                    Id = identity,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    StrengthMax = 3,
                    StrengthMin = 2,
                    VitalityMax = 5,
                    VitalityMin = 5,
                    WillpowerMax = 2,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 1,
                            SkillLevelMin = 1,
                            GuaranteedSuccesses = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        }
                    }
                }
            });
        }

        public async Task AddHyena(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Hiéna",
                    Name = "Hyena",
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
                    WillpowerMin = 1,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        }
                    }
                }
            });
        }

        public async Task AddVenomousSnake(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Mérges kígyó",
                    Name = "Venomous snake",
                    AgilityMax = 5,
                    AgilityMin = 3,
                    BodyMax = 2,
                    BodyMin = 1,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    DexterityMax = 5,
                    DexterityMin = 3,
                    Difficulty = Difficulty.Novice,
                    EmotionMax = 0,
                    EmotionMin = 0,
                    IntelligenceMax = 0,
                    IntelligenceMin = 0,
                    IsUndead = false,
                    StrengthMax = 1,
                    StrengthMin = 2,
                    VitalityMax = 3,
                    VitalityMin = 3,
                    WillpowerMax = 2,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 36,
                            SkillLevelMax = 3,
                            SkillLevelMin = 2,
                            GuaranteedSuccesses = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        }
                    }
                }
            });
        }

        public async Task AddNonVenomousSnake(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Nem mérges kígyó",
                    Name = "Non-venomous snake",
                    AgilityMax = 3,
                    AgilityMin = 2,
                    BodyMax = 5,
                    BodyMin = 3,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    DexterityMax = 5,
                    DexterityMin = 3,
                    Difficulty = Difficulty.Novice,
                    EmotionMax = 0,
                    EmotionMin = 0,
                    IntelligenceMax = 0,
                    IntelligenceMin = 0,
                    IsUndead = false,
                    StrengthMax = 7,
                    StrengthMin = 3,
                    VitalityMax = 3,
                    VitalityMin = 3,
                    WillpowerMax = 2,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 36,
                            SkillLevelMax = 3,
                            SkillLevelMin = 2,
                            GuaranteedSuccesses = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 5,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        }
                    }
                }
            });
        }

        public async Task AddCrocodile(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Krokodil",
                    Name = "Crocodile",
                    AgilityMax = 4,
                    AgilityMin = 3,
                    BodyMax = 8,
                    BodyMin = 4,
                    DamageReductionMax = 4,
                    DamageReductionMin = 2,
                    DexterityMax = 3,
                    DexterityMin = 2,
                    Difficulty = Difficulty.Talented,
                    EmotionMax = 0,
                    EmotionMin = 0,
                    IntelligenceMax = 0,
                    IntelligenceMin = 0,
                    IsUndead = false,
                    StrengthMax = 7,
                    StrengthMin = 3,
                    VitalityMax = 7,
                    VitalityMin = 7,
                    WillpowerMax = 3,
                    WillpowerMin = 3,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 5,
                            SkillLevelMin = 5
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 36,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 4,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 41
                        }
                    },
                    CreatureMerits = new List<CreatureMerit>
                    {
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 26
                        }
                    }
                }
            });
        }

        public async Task AddDog(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Kutya",
                    Name = "Dog",
                    AgilityMax = 4,
                    AgilityMin = 3,
                    BodyMax = 3,
                    BodyMin = 2,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    DexterityMax = 5,
                    DexterityMin = 4,
                    Difficulty = Difficulty.Novice,
                    EmotionMax = 2,
                    EmotionMin = 1,
                    IntelligenceMax = 2,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 3,
                    StrengthMin = 2,
                    VitalityMax = 4,
                    VitalityMin = 4,
                    WillpowerMax = 2,
                    WillpowerMin = 1,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 4,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 40
                        }
                    }
                }
            });
        }
        
        public async Task AddHeavyHorse(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Igás- és nehéz csataló",
                    Name = "Cart hourse and destrier",
                    AgilityMax = 4,
                    AgilityMin = 3,
                    BodyMax = 6,
                    BodyMin = 5,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    DexterityMax = 5,
                    DexterityMin = 4,
                    Difficulty = Difficulty.Beginner,
                    EmotionMax = 2,
                    EmotionMin = 1,
                    IntelligenceMax = 2,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 8,
                    StrengthMin = 7,
                    VitalityMax = 4,
                    VitalityMin = 4,
                    WillpowerMax = 2,
                    WillpowerMin = 1,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 3,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 22,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 3,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 43
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 44
                        }
                    },
                    CreatureMerits = new List<CreatureMerit>
                    {
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 26
                        }
                    }
                }
            });
        }

        public async Task AddLightHorse(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Utazó ló és könnyű csataló",
                    Name = "Steed",
                    AgilityMax = 6,
                    AgilityMin = 5,
                    BodyMax = 6,
                    BodyMin = 5,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    DexterityMax = 5,
                    DexterityMin = 4,
                    Difficulty = Difficulty.Beginner,
                    EmotionMax = 2,
                    EmotionMin = 1,
                    IntelligenceMax = 2,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 6,
                    StrengthMin = 5,
                    VitalityMax = 4,
                    VitalityMin = 4,
                    WillpowerMax = 2,
                    WillpowerMin = 1,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 3,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 22,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 3,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureMerits = new List<CreatureMerit>
                    {
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 26
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 43
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 44
                        }
                    }
                }
            });
        }

        public async Task AddCat(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Macska",
                    Name = "Cat",
                    AgilityMax = 6,
                    AgilityMin = 6,
                    BodyMax = 1,
                    BodyMin = 1,
                    DamageReductionMax = 0,
                    DamageReductionMin = 0,
                    DexterityMax = 7,
                    DexterityMin = 6,
                    Difficulty = Difficulty.Novice,
                    EmotionMax = 1,
                    EmotionMin = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 1,
                    StrengthMin = 1,
                    VitalityMax = 3,
                    VitalityMin = 3,
                    WillpowerMax = 3,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 1,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 5,
                            SkillLevelMin = 3
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

        public async Task AddBear(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Medve",
                    Name = "Bear",
                    AgilityMax = 5,
                    AgilityMin = 4,
                    BodyMax = 8,
                    BodyMin = 5,
                    DamageReductionMax = 2,
                    DamageReductionMin = 2,
                    DexterityMax = 4,
                    DexterityMin = 3,
                    Difficulty = Difficulty.Talented,
                    EmotionMax = 1,
                    EmotionMin = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 7,
                    StrengthMin = 5,
                    VitalityMax = 5,
                    VitalityMin = 5,
                    WillpowerMax = 3,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 20,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 1,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 3,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 41
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 44
                        }
                    },
                    CreatureMerits = new List<CreatureMerit>
                    {
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 26
                        }
                    }
                }
            });
        }

        public async Task AddLion(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Oroszlán",
                    Name = "Lion",
                    AgilityMax = 3,
                    AgilityMin = 2,
                    BodyMax = 7,
                    BodyMin = 5,
                    DexterityMax = 6,
                    DexterityMin = 4,
                    Difficulty = Difficulty.Talented,
                    EmotionMax = 1,
                    EmotionMin = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 7,
                    StrengthMin = 5,
                    VitalityMax = 5,
                    VitalityMin = 5,
                    WillpowerMax = 3,
                    WillpowerMin = 3,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 1,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 3,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 41
                        },
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 44
                        }
                    }
                }
            });
        }

        public async Task AddSpider(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Pók",
                    Name = "Spider",
                    AgilityMax = 6,
                    AgilityMin = 6,
                    BodyMax = 7,
                    BodyMin = 1,
                    DexterityMax = 6,
                    DexterityMin = 6,
                    Difficulty = Difficulty.Talented,
                    EmotionMax = 0,
                    EmotionMin = 0,
                    IntelligenceMax = 0,
                    IntelligenceMin = 0,
                    IsUndead = false,
                    StrengthMax = 6,
                    StrengthMin = 1,
                    VitalityMax = 3,
                    VitalityMin = 3,
                    WillpowerMax = 2,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 20,
                            SkillLevelMax = 5,
                            SkillLevelMin = 1,
                            GuaranteedSuccesses = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
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

        public async Task AddReu(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Reu",
                    Name = "Reu",
                    AgilityMax = 2,
                    AgilityMin = 2,
                    BodyMax = 10,
                    BodyMin = 10,
                    DexterityMax = 2,
                    DexterityMin = 2,
                    DamageReductionMax = 6,
                    DamageReductionMin = 4,
                    Difficulty = Difficulty.Seasoned,
                    EmotionMax = 0,
                    EmotionMin = 0,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 10,
                    StrengthMin = 10,
                    VitalityMax = 10,
                    VitalityMin = 10,
                    WillpowerMax = 3,
                    WillpowerMin = 3,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 4,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        }
                    },
                    CreatureMerits = new List<CreatureMerit>
                    {
                        new CreatureMerit
                        {
                            CreatureId = identity,
                            MeritId = 26
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 44
                        }
                    }
                }
            });
        }

        public async Task AddScorpion(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Skorpió",
                    Name = "Scorpion",
                    AgilityMax = 5,
                    AgilityMin = 4,
                    BodyMax = 0,
                    BodyMin = 0,
                    DexterityMax = 4,
                    DexterityMin = 4,
                    Difficulty = Difficulty.Newbie,
                    EmotionMax = 0,
                    EmotionMin = 0,
                    IntelligenceMax = 0,
                    IntelligenceMin = 0,
                    IsUndead = false,
                    StrengthMax = 0,
                    StrengthMin = 0,
                    VitalityMax = 4,
                    VitalityMin = 4,
                    WillpowerMax = 2,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 1,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 36,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
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

        public async Task AddTiger(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Tigris",
                    Name = "Tiger",
                    AgilityMax = 5,
                    AgilityMin = 4,
                    BodyMax = 6,
                    BodyMin = 4,
                    DexterityMax = 5,
                    DexterityMin = 4,
                    Difficulty = Difficulty.Talented,
                    EmotionMax = 1,
                    EmotionMin = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 6,
                    StrengthMin = 4,
                    VitalityMax = 4,
                    VitalityMin = 4,
                    WillpowerMax = 3,
                    WillpowerMin = 3,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 19,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 20,
                            SkillLevelMax = 1,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 2,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 3,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2,
                            GuaranteedSuccesses = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2,
                            GuaranteedSuccesses = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 3,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 41
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

        public async Task AddFalcon(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Vadászsólyom",
                    Name = "Falcon",
                    AgilityMax = 6,
                    AgilityMin = 4,
                    BodyMax = 1,
                    BodyMin = 1,
                    DexterityMax = 6,
                    DexterityMin = 4,
                    Difficulty = Difficulty.Beginner,
                    EmotionMax = 1,
                    EmotionMin = 1,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 1,
                    StrengthMin = 1,
                    VitalityMax = 2,
                    VitalityMin = 2,
                    WillpowerMax = 3,
                    WillpowerMin = 2,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 1,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 69, //Flying
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 1,
                            SkillLevelMax = 4,
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
                        }
                    }
                }
            });
        }

        public async Task AddWarg(DbContext dbContext, int identity)
        {
            await dbContext.BulkInsertOrUpdateAsync(new[]
            {
                new Creature
                {
                    Id = identity,
                    NameHu = "Warg",
                    Name = "Warg",
                    AgilityMax = 5,
                    AgilityMin = 4,
                    BodyMax = 6,
                    BodyMin = 6,
                    DexterityMax = 5,
                    DexterityMin = 4,
                    DamageReductionMax = 1,
                    DamageReductionMin = 1,
                    Difficulty = Difficulty.Rookie,
                    EmotionMax = 0,
                    EmotionMin = 0,
                    IntelligenceMax = 1,
                    IntelligenceMin = 1,
                    IsUndead = false,
                    StrengthMax = 6,
                    StrengthMin = 4,
                    VitalityMax = 5,
                    VitalityMin = 5,
                    WillpowerMax = 3,
                    WillpowerMin = 3,
                    CreatureSkills = new List<CreatureSkill>
                    {
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 21,
                            SkillLevelMax = 3,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 32,
                            SkillLevelMax = 4,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 34,
                            SkillLevelMax = 3,
                            SkillLevelMin = 1
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 40,
                            SkillLevelMax = 5,
                            SkillLevelMin = 2
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 3,
                            SkillLevelMax = 4,
                            SkillLevelMin = 3
                        },
                        new CreatureSkill
                        {
                            CreatureId = identity,
                            SkillId = 12,
                            SkillLevelMax = 2,
                            SkillLevelMin = 1
                        }
                    },
                    CreatureWeapons = new List<CreatureWeapon>
                    {
                        new CreatureWeapon
                        {
                            CreatureId = identity,
                            WeaponId = 41
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

        private static int GetIdentity()
        {
             return _identity++;
        }
    }
}