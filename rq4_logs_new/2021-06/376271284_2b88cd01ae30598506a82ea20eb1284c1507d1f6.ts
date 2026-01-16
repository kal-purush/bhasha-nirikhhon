interface abilitiesPropertiesTypes {
    abilityName: string;
    abilityScoreValue: number;
    abilityChecks: string[];
}

export const abilitiesProperties: abilitiesPropertiesTypes[] = [
    {abilityName: "strength", abilityScoreValue: 18, abilityChecks: ["Athletics"]},
    {abilityName: "dexterity", abilityScoreValue: 16, abilityChecks: ["Acrobatics", "Sleight of Hand", "Stealth"]},
    {abilityName: "constitution", abilityScoreValue: 12, abilityChecks: []},
    {abilityName: "intelligence", abilityScoreValue: 10, abilityChecks: ["Arcana", "History", "Investigation", "Nature", "Religion"]},
    {abilityName: "wisdom", abilityScoreValue: 10, abilityChecks: ["Animal Handling", "Insight", "Medicine", "Perception", "Survival"]},
    {abilityName: "charisma", abilityScoreValue: 8, abilityChecks: ["Deception", "Intimidation", "Performance", "Persuasion"]}
];