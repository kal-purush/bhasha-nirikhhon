package book.store.intro.controller;

import book.store.intro.dto.shoppingcart.request.CreateCartItemRequestDto;
import book.store.intro.dto.shoppingcart.request.UpdateCartItemRequestDto;
import book.store.intro.dto.shoppingcart.response.CartItemResponseDto;
import book.store.intro.dto.shoppingcart.response.ShoppingCartResponseDto;
import book.store.intro.service.shoppingcart.CartItemService;
import book.store.intro.service.shoppingcart.ShoppingCartService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Shopping Cart management",
        description = "Endpoints for managing shopping cart and cart items")
@RestController
@RequestMapping("/api/cart")
@RequiredArgsConstructor
public class ShoppingCartController {
    private final ShoppingCartService shoppingCartService;
    private final CartItemService cartItemService;

    @GetMapping
    @Operation(summary = "Get shopping cart", description = "Get shopping cart with all cart items")
    public ShoppingCartResponseDto getShoppingCart() {
        return shoppingCartService.getShoppingCart();
    }

    @PostMapping
    @Operation(summary = "Add cart item", description = "Add cart item to the shopping cart")
    public CartItemResponseDto addCartItem(@RequestBody CreateCartItemRequestDto requestDto) {
        return cartItemService.addCartItem(requestDto);
    }

    @PutMapping("/cart-items/{id}")
    @Operation(summary = "Update quantity", description = "Update quantity in cart item")
    public CartItemResponseDto updateQuantity(@PathVariable Long id,
                                              @RequestBody UpdateCartItemRequestDto requestDto) {
        return cartItemService.updateQuantity(id, requestDto);
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete cart item", description = "Delete cart item by id")
    @DeleteMapping("/cart-items/{id}")
    public void delete(@PathVariable Long id) {
        cartItemService.deleteById(id);
    }
}