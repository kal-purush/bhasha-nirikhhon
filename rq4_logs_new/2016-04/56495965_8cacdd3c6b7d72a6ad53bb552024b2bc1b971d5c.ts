
export interface ICard {
    id: string;
    name: string;
    extensionSet: ExtensionSet;
    cost: ICardCost;
    type: CardType[];
    capabilities: ICardCapabilities;
}

export enum CardType {
    action,
    reaction,
    treasure,
    duration,
    attack,
    victory,
    looter,
    ruins,
    knight,
    shelter
}

export enum ExtensionSet {
    base,
    intrigue,       //intrigue
    seaside,        //rivages
    alchemy,        //alchmie
    prosperity,     //prosperite
    cornucopia,     //abondance
    hinterlands,    //larrierepays
    dark_ages,      //agedestenebres
    guilds,         //guildes
    adventures,     //aventures
    promotional
}

export interface ICardCost {
    money: number;
    potion: number;
}

export interface IPlayer {
    name: string;
    gameHistory: {
        
    }
}