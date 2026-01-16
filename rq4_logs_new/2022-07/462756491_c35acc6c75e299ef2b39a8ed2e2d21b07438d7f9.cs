using CthulhuWizard.Persistence.DefaultData.Skills;
using CthulhuWizard.Persistence.Models;
using CthulhuWizard.Persistence.Models.Investigators;
using CthulhuWizard.Persistence.Models.Occupations;

namespace CthulhuWizard.Persistence.DefaultData.Occupations; 

public static class Antiquarian {
    public static OccupationEntity Seed() {
        return new() {
            Id = Guid.NewGuid().ToString(),
            Name = "Antiquarian",
            Descritpion =
                "A person who delights in the timeless excellence of design and execution, and in the power " +
                "of ancient lore. Probably the most Lovecraft-like occupation available to an investigator. ",
            ImageUrl = "",
            Skills = new List<OccupationSkillSpecificationEntity> {
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.Appraise
                    }
                },
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.ArtCraft.Any
                    }
                },
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.History
                    }
                },
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.LibraryUse
                    }
                },
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.OtherLanguage.Any
                    }
                },
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.Charm,
                        SkillNames.FastTalk,
                        SkillNames.Intimidate,
                        SkillNames.Persuade
                    }
                },
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.SpotHidden
                    }
                },
                new() {
                    HowMany = 1,
                    From = new List<string> {
                        SkillNames.Any
                    }
                },
            },
            IsLovecraftian = true,
            SkillPoints = 0,
            MinCreditRating = 30,
            MaxCreditRating = 70,
            SkillPointsPattern = new List<SkillPointsPatternEntity> {
                new() {
                    Multiplier = 4,
                    PossibleAttributes = new List<string> {
                        nameof(CharacteristicEntity.Education)
                    }
                }
            },
            SuggestedContacts = "Booksellers, antique collectors, historical societies"
        };
    }
}