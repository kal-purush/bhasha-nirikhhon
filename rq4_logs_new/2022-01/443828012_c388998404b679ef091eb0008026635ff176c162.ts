export enum PepperColor {
    "Red",
    "Blue",
    "Yellow",
    "Green",
    "Orange",
    "Purple",
    "Brown",
    "White",
    "Black",
    "Ghost",
}
export enum TimeOfDay {
    "MORNING",
    "AFTERNOON"
}
export type PlayerColor = "Red"|"Blue"|"Yellow"|"Green"|"Orange"|"Purple"
export type Role = "Path"|"Harvest"|"Plant"
export type BonusAction = "ExtraPlant"|"ExtraMove"|"TurnAround"
export type PlaqueColor = "Secondary"|"Brown"|"Black"|"White"|"Ghost"

export interface Pepper {
    kind: 'pepper'
    color: PepperColor
}

export interface Recipe {
    kind: 'recipe'
    ingredients: Pepper[]
    pointValue: number
}

export interface Plaque {
    kind: 'plaque'
    pointValue: number
    color: PlaqueColor
}
export interface MarketCard {
    kind: 'marketCard'
    time: TimeOfDay
    pointValue: number
    getPepperReward?: (arg0?: PepperColor) => Pepper
    peppersRequired: Pepper[]
    moneyReward: number
}
export interface AuctionCard {
    kind: 'auctionCard'
    time: TimeOfDay
    peppers: Pepper[]
}
export interface Player {
    kind: 'player';
    color: PlayerColor;
    peppers: Pepper[];
    score: number;
    money: number;
    actions: BonusAction[];
    recipiesCompleted: Recipe[];
    plaques: Plaque[];
    marketCards: MarketCard[];
}

export interface Point {
    x: number;
    y: number;
}
  