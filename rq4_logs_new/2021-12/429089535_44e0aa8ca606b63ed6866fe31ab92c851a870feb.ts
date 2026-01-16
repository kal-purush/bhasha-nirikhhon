import { addGameToCart, removeGameFromCart, removeGamesFromCart } from "./actionTypesCart";
import { GameCart, CartAction } from "../../types/types";

export const addGameToCartAction = (game: GameCart): CartAction => ({
  type: addGameToCart,
  payload: game,
});

export const removeGameFromCartAction = (game: GameCart): CartAction => ({
  type: removeGameFromCart,
  payload: game,
});

export const removeGamesFromCartAction = (games: Array<GameCart>): CartAction => ({
  type: removeGamesFromCart,
  payload: games,
});