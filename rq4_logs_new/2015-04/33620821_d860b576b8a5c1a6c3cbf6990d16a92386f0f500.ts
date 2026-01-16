/**
 * Created by leh on 07.04.2015.
 */
// Generated from https://github.com/lean-poker/poker-croupier/wiki/Player-API
export interface GameState
{
    players: Player[];
    small_blind: number;
    orbits: number;
    dealer: number;
    community_cards: Card[];
    current_buy_in: number;
    pot: number;
}

export interface Card {
    rank:number;
    suit: string;
}

export interface Player
{
    name: string;
    stack: number;
    status: string
    bet: number;
    hole_cards: Card[];
    version: string;
    id: number;
}