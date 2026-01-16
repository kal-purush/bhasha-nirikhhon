/* Shared */

declare var __DEVTOOLS__: boolean;

declare module "gmbuddy/campaign" {
    export interface ICampaign {
        gameType: string;
        campaignId: string;
        name: string;
        gmUserId: string;
    }

    export interface ICampaignData {
        gameType: string;
        name: string;
    }
}

/* Generics */

declare module "gmbuddy/character" {
    export interface ICharacter {
        characterId?: number;
        gameType?: String;
        details: {
            characterId: string;
            name: string;
            userId: string;
        };
    }
}

/* D&D 3.5 */
declare module "gmbuddy/dnd35/character" {
    export interface ICharacterDetails {
        userId: string;
        characterId: string;
        name: string;
        class: string;
        race: string;
        alignment: string;
        deity: string;
        size: string;
        age: string;
        gender: string;
        height: string;
        weight: string;
        eyes: string;
        hair: string;
        skin: string;
    }

    export interface ICharacterStats {
        stats: Object;
    }

    export interface ICharacterItem {
        type: string;
        name: string;
        damageDieAmount: number;
        damageDie: number;
        damageType: string;
        twoHands: boolean;
        weight: number;
        range: number;
    }

    export interface ICharacterItems {
        items: ICharacterItem[];
    }

    export interface ICharacterSkills {
        skills: Object;
    }

    export interface ICharacterData {
        characterId?: number;
        gameType?: String;
        details: ICharacterDetails;
        stats: ICharacterStats;
        items: ICharacterItems;
        skills: ICharacterSkills;
    }
}

/* Microlite20 */
declare module "gmbuddy/micro20/character" {
    export interface ICharacterDetails {
        userId: string;
        characterId: string;
        name: string;
        class: string;
        race: string;
        height: string;
        weight: string
        hairColor: string;
        eyeColor: string;
    }

    export interface ICharacterStats {
        strength: number;
        dexterity: number;
        mind: number;
    }

    export interface ICharacterItem {
        type: string;
        name: string;
        damageDieAmount: number;
        damageDie: number;
        damageType: string;
        twoHands: boolean;
        weight: number;
        range: number;
    }

    export interface ICharacterItems {
        items: ICharacterItem[];
    }

    export interface ICharacterSkills {
        skills: Object;
    }

    export interface ICharacterSpell {
        level: number;
        type: string;
        name: string;
        description: string;
    }

    export interface ICharacterSpells {
        spells: ICharacterSpell[];
    }

    export interface ICharacterData {
        characterId?: number;
        gameType?: string;
        details: ICharacterDetails;
        baseStats: ICharacterStats;
        modifiers: ICharacterStats;
    }
}