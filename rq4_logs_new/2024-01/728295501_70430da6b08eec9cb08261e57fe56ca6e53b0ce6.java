package study.tipsyboy.tipsyboyMall.cart.api;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import study.tipsyboy.tipsyboyMall.auth.dto.LoginMember;
import study.tipsyboy.tipsyboyMall.cart.dto.CartItemResponseDto;
import study.tipsyboy.tipsyboyMall.cart.dto.CartItemSaveRequestDto;
import study.tipsyboy.tipsyboyMall.cart.dto.CartItemUpdateRequestDto;
import study.tipsyboy.tipsyboyMall.cart.service.CartItemService;

import java.util.List;

@Slf4j
@RequestMapping("/api/cartItem")
@RequiredArgsConstructor
@RestController
public class CartItemApiController {

    private final CartItemService cartItemService;

    @PostMapping
    @PreAuthorize("hasAnyRole('ROLE_ADMIN', 'ROLE_MEMBER')")
    public ResponseEntity<Long> addToCart(@RequestBody CartItemSaveRequestDto requestDto,
                                          @AuthenticationPrincipal LoginMember loginMember) {
        return ResponseEntity.ok(cartItemService.save(loginMember.getMemberId(), requestDto));
    }

    @GetMapping
    @PreAuthorize("hasAnyRole('ROLE_ADMIN', 'ROLE_MEMBER')")
    public ResponseEntity<List<CartItemResponseDto>> getCartItems(@AuthenticationPrincipal LoginMember loginMember) {
        return ResponseEntity.ok(cartItemService.readCartItems(loginMember.getMemberId()));
    }

    @PatchMapping("/{cartItemId}")
    @PreAuthorize("hasRole('ROLE_ADMIN') || (hasRole('ROLE_MEMBER') && hasPermission(#cartItemId, 'CART_ITEM', 'PATCH'))")
    public ResponseEntity<Void> editCartItemCount(@PathVariable Long cartItemId,
                                                  @RequestBody CartItemUpdateRequestDto requestDto,
                                                  @AuthenticationPrincipal LoginMember loginMember) {
        cartItemService.editCartItemCount(loginMember.getMemberId(), cartItemId, requestDto);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{cartItemId}")
    @PreAuthorize("hasRole('ROLE_ADMIN') || (hasRole('ROLE_MEMBER') && hasPermission(#cartItemId, 'CART_ITEM', 'DELETE'))")
    public ResponseEntity<Void> deleteCartItem(@PathVariable Long cartItemId,
                                               @AuthenticationPrincipal LoginMember loginMember) {
        cartItemService.deleteCartItem(cartItemId);
        return ResponseEntity.ok().build();
    }
}