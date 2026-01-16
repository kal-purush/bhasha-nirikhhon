import { useContext } from "react";
import { WishlistContext } from "./WishlistContext";

export const useWishlist = () => {
  const context = useContext(WishlistContext);
  if (!context) {
    throw new Error('useWishlist must be used inside a WishlistProvider');
  }
  return context;
};