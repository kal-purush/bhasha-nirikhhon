import { Card } from "./card.interface";

export interface Deck {
  _id: string;
  title: string;
  cards: Card[]; // Update with the correct type for cards if available
  created_by: string;
  visibility: string;
  is_blocked: boolean;
  favorites?: any[]; // Update with correct type if available
  createdAt: string;
  updatedAt: string;
  __v: number;
}

export interface DecksResponse {
  statuscode: number;
  message: string;
  data: Deck[];
  success: boolean;
}

export interface SingleDecksResponse {
  statuscode: number;
  message: string;
  data: Deck;
  success: boolean;
}