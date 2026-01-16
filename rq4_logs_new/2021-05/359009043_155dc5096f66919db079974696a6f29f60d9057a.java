package ru.beerbis.springer.controller;

import org.springframework.web.bind.annotation.*;
import ru.beerbis.springer.repo.error.EntityNotFound;
import ru.beerbis.springer.service.cart.Cart;
import ru.beerbis.springer.service.cart.CartItem;

import java.util.List;

@RestController
@RequestMapping("/api-v1/cart")
public class CartRestController {

    private final Cart cart;

    public CartRestController(Cart cart) {
        this.cart = cart;
    }

    @GetMapping
    public List<CartItem> all() {
        return cart.listAll();
    }

    @GetMapping("/{productId}")
    public CartItem get(@PathVariable Integer productId) {
        return cart.get(productId).orElseThrow(() -> new EntityNotFound(CartItem.class, productId));
    }

    @PostMapping("/{productId}")
    public void set(@PathVariable Integer productId,
                    @RequestParam Integer count) {
        cart.set(productId, count);
    }

    @DeleteMapping("/{productId}")
    public Integer delete(@PathVariable Integer productId,
                          @RequestParam(required = false) Integer count) {
        if (count != null) {
            return cart.remove(productId, count);
        } else
        if (!cart.remove(productId)) {
            throw new EntityNotFound(CartItem.class, productId);
        }
        return 0;
    }

    @PutMapping("/{productId}")
    public CartItem put(@PathVariable Integer productId,
                        @RequestParam(required = false) Integer count) {
        return new CartItem(productId, count == null
                ? cart.put(productId)
                : cart.put(productId, count));
    }
}