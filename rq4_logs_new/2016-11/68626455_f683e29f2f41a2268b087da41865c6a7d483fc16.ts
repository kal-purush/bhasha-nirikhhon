/* Shared */
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

/* D&D 3.5 */
declare module "gmbuddy/dnd35/character" {
    export interface ICharacterDetails {
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
        details: ICharacterDetails;
        stats: ICharacterStats;
        items: ICharacterItems;
        skills: ICharacterSkills;
    }
}

/* Microlite20 */
declare module "gmbuddy/micro20/character" {
    export interface ICharacterDetails {
        name: string;
        class: string;
        race: string;
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
        details: ICharacterDetails;
        items: ICharacterItems;
        skills: ICharacterSkills;
        spells: ICharacterSpells;
    }
}