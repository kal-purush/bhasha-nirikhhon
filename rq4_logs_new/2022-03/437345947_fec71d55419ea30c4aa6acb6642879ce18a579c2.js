export const ADD_TO_FAVORITE = "ADD_TO_FAVORITE";
export const REMOVE_FROM_FAVORITE = "REMOVE_FROM_FAVORITE";

export function addToFavorite(beer) {
  return {
    type: ADD_TO_FAVORITE,
    payload: beer,
  };
}

export function removeFromFavorite(beer) {
  return {
    type: REMOVE_FROM_FAVORITE,
    payload: beer,
  };
}