import { useEffect } from "react";
import { type Product } from "~/lib/schemas/product";
import { useCart } from "~/lib/store";
import { api } from "~/trpc/react";

export default function useHotkeys({
  ref,
  handleBuy,
  activateHotkeys = false,
}: {
  ref: React.RefObject<HTMLElement>;
  handleBuy: () => void;
  activateHotkeys?: boolean;
}) {
  const { data: products  } = api.product.getAll.useQuery();
  const { addToCart, removeFromCart, reset } = useCart();

  // Add hotkeys to add products to cart
  useEffect(() => {
    const hotkeys =
      products?.reduce((acc, product) => {
        if (product.hotkey) {
          acc[product.hotkey] = {
            add: () => addToCart(product),
            remove: () => removeFromCart(product),
            product,
          };
        }
        return acc;
      }, {} as Record<string, { add: () => void; remove: () => void; product: Product }>) ??
      {};

    const handleKeyDown = (e: KeyboardEvent) => {
      if (ref.current?.contains(e.target as Node)) return;
      const keyPressed = e.key.toLocaleLowerCase();
      if (keyPressed === "f1") {
        e.preventDefault();
        handleBuy();
        return;
      }

      const key = hotkeys[keyPressed];
      if (!key) return;
      if (e.shiftKey) key.remove();
      else key.add();
      const elementToScroll = document.getElementById(`p-${key.product.id}`);
      elementToScroll?.scrollIntoView({
        behavior: "smooth",
        block: "center",
      });
    };

    window.addEventListener("keydown", handleKeyDown);

    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [products, addToCart, removeFromCart, handleBuy, reset, ref]);
}