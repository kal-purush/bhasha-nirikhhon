package bookstore.controller;

import bookstore.dto.cartitem.CreateCartItemRequestDto;
import bookstore.dto.cartitem.UpdateCartItemRequestDto;
import bookstore.dto.shoppingcart.ShoppingCartResponseDto;
import bookstore.service.ShoppingCartService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Shopping cart management",
        description = "Endpoints for managing shopping cart and its cart items")
@RequiredArgsConstructor
@RestController
@RequestMapping(value = "/cart")
public class ShoppingCartController {
    private final ShoppingCartService shoppingCartService;

    @Operation(summary = "Get user's shopping cart",
            description = "Get user's shopping cart with cart items")
    @GetMapping
    @PreAuthorize("hasRole('USER')")
    public ShoppingCartResponseDto getShoppingCart(Authentication authentication) {
        String email = authentication.getName();
        return shoppingCartService.getShoppingCart(email);
    }

    @Operation(summary = "Add book to user's shopping cart",
            description = "Get user's shopping cart with cart items")
    @PostMapping
    @PreAuthorize("hasRole('USER')")
    public ShoppingCartResponseDto addBookToShoppingCart(
            Authentication authentication,
            @RequestBody @Valid CreateCartItemRequestDto requestDto
    ) {
        String email = authentication.getName();
        return shoppingCartService.addCartItem(email, requestDto);
    }

    @Operation(summary = "Update item of user's shopping cart",
            description = "Update item of user's shopping cart")
    @PutMapping("cart-items/{cartItemId}")
    @PreAuthorize("hasRole('USER')")
    public ShoppingCartResponseDto updateCartItem(
            Authentication authentication,
            @PathVariable Long cartItemId,
            @RequestBody @Valid UpdateCartItemRequestDto requestDto
    ) {
        String email = authentication.getName();
        return shoppingCartService.updateCartItem(email, cartItemId, requestDto);
    }

    @Operation(summary = "Delete item from user's shopping cart",
            description = "Delete item from user's shopping cart")
    @DeleteMapping("cart-items/{cartItemId}")
    @PreAuthorize("hasRole('USER')")
    public ShoppingCartResponseDto deleteCartItem(
            Authentication authentication,
            @PathVariable Long cartItemId
    ) {
        String email = authentication.getName();
        return shoppingCartService.deleteCartItem(email, cartItemId);
    }
}