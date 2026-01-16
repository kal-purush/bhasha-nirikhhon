import mongoose from "mongoose";
import * as cartService from "../modules/cart/cart.service";
import * as productService from "../modules/product/product.service";

/**
 * Get user's cart with product snapshots including current and historical prices
 * Useful for cart UIs and checkout previews
 *
 * @param userId The user ID
 * @returns Cart with complete product information and price history
 */
export const getUserCartWithProductSnapshot = async (userId: string) => {
  try {
    // Get active cart for user
    const cart = await cartService.getActiveCartByUserId(userId);

    if (!cart) {
      return {
        cart: null,
        items: [],
        subtotal: 0,
        total_items: 0,
      };
    }

    // Get cart items
    const cartItems = await cartService.getCartItems(cart.id);

    if (!cartItems || cartItems.length === 0) {
      return {
        cart: {
          id: cart.id,
          user_id: cart.user_id,
          created_at: cart.created_at,
          updated_at: cart.updated_at,
        },
        items: [],
        subtotal: 0,
        total_items: 0,
      };
    }

    // Get product IDs from cart items
    const productIds = cartItems.map((item) => item.product_id);

    // Fetch products in bulk
    const products = await productService.getProductsByIds(
      productIds.map((id) => id.toString())
    );

    // Map products to a dictionary for fast lookups
    const productMap = {};
    products.forEach((product) => {
      productMap[product.id] = product;
    });

    // Combine cart items with product details
    const enrichedItems = cartItems.map((item) => {
      const product = productMap[item.product_id.toString()];

      // Handle case where product might have been deleted or is unavailable
      if (!product) {
        return {
          item_id: item.id,
          product_id: item.product_id,
          quantity: item.quantity,

          product_available: false,
          snapshot: {
            name: "Product no longer available",
            price: (item as any).unit_price || 0,
            image_url: "",
            price_at_addition: (item as any).unit_price || 0,
            current_price: 0,
            price_difference: 0,
            price_changed: false,
          },
        };
      }

      // Calculate price differences
      const priceAtAddition = (item as any).unit_price || product.price;
      const currentPrice = product.price;
      const priceDifference = currentPrice - priceAtAddition;

      return {
        item_id: item.id,
        product_id: item.product_id,
        quantity: item.quantity,

        product_available:
          product.is_available && product.available_stock >= item.quantity,
        stock_warning: product.available_stock < item.quantity,
        snapshot: {
          name: product.name,
          description: product.description,
          price: priceAtAddition, // Price used for this cart item
          image_url:
            product.images && product.images.length > 0
              ? product.images[0]
              : "",
          brand: product.brand,
          category: product.category,
          price_at_addition: priceAtAddition,
          current_price: currentPrice,
          price_difference: priceDifference,
          price_changed: Math.abs(priceDifference) > 0.01, // Allow for small floating point differences
          discount: product.discount || 0,
          discount_type: product.discount_type || "fixed",
        },
      };
    });

    // Calculate subtotal
    const subtotal = enrichedItems.reduce((total, item) => {
      if (!item.product_available) return total;

      const itemPrice = item.snapshot.price;
      let itemDiscount = 0;

      // Apply discounts
      if (item.snapshot.discount) {
        if (item.snapshot.discount_type === "percentage") {
          itemDiscount = (itemPrice * item.snapshot.discount) / 100;
        } else {
          itemDiscount = item.snapshot.discount;
        }
      }

      const finalPrice = Math.max(0, itemPrice - itemDiscount);
      return total + finalPrice * item.quantity;
    }, 0);

    // Count total items
    const totalItems = enrichedItems.reduce((count, item) => {
      return count + item.quantity;
    }, 0);

    // Return complete cart data
    return {
      cart: {
        id: cart.id,
        user_id: cart.user_id,
        created_at: cart.created_at,
        updated_at: cart.updated_at,
      },
      items: enrichedItems,
      subtotal: subtotal,
      total_items: totalItems,
      has_unavailable_items: enrichedItems.some(
        (item) => !item.product_available
      ),
      has_price_changes: enrichedItems.some(
        (item) => item.snapshot.price_changed
      ),
    };
  } catch (error) {
    throw new Error(
      `Error fetching cart with product snapshot: ${error.message}`
    );
  }
};

/**
 * Get cart with product snapshot by cart ID
 *
 * @param cartId The cart ID
 * @returns Cart with complete product information and price history
 */
export const getCartWithProductSnapshot = async (cartId: string) => {
  try {
    // Get cart by ID
    const cart = await cartService.getCartById(cartId);

    if (!cart) {
      throw new Error("Cart not found");
    }

    // Use userId from the cart to fetch the full cart data
    return await getUserCartWithProductSnapshot(cart.user_id.toString());
  } catch (error) {
    throw new Error(
      `Error fetching cart with product snapshot: ${error.message}`
    );
  }
};