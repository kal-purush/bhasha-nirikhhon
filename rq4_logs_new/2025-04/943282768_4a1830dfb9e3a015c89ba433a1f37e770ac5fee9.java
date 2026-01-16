package io.jeidiiy.sirenordersystem.cart.controller;

import io.jeidiiy.sirenordersystem.cart.domain.dto.CartPostRequestDto;
import io.jeidiiy.sirenordersystem.cart.domain.dto.CartResponseDto;
import io.jeidiiy.sirenordersystem.cart.service.CartService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@Tag(name = "장바구니 API")
@RequiredArgsConstructor
@RequestMapping("/api/v1/carts")
@RestController
public class CartController {

  private final CartService cartService;

  @Operation(summary = "특정 사용자의 장바구니 조회하기")
  @PreAuthorize("#username == authentication.name")
  @GetMapping("/{username}")
  public ResponseEntity<List<CartResponseDto>> get(@PathVariable String username) {
    return ResponseEntity.ok(cartService.findByUsername(username));
  }

  @Operation(summary = "특정 사용자의 장바구니 변경하기")
  @PreAuthorize("#username == authentication.name")
  @PostMapping("/{username}")
  public ResponseEntity<List<CartResponseDto>> upsert(
      @PathVariable String username, @RequestBody CartPostRequestDto cartPostRequestDto) {
    return ResponseEntity.ok(cartService.upsert(username, cartPostRequestDto));
  }

  @Operation(summary = "특정 사용자의 장바구니 특정 목록 삭제하기")
  @PreAuthorize("#username == authentication.name")
  @DeleteMapping("/{username}/{cartId}")
  public ResponseEntity<List<CartResponseDto>> remove(
      @PathVariable String username, @PathVariable Integer cartId) {
    return ResponseEntity.ok(cartService.remove(username, cartId));
  }

  @Operation(summary = "특정 사용자의 장바구니 모든 목록 삭제하기")
  @PreAuthorize("#username == authentication.name")
  @DeleteMapping("/{username}")
  public ResponseEntity<List<CartResponseDto>> removeAll(@PathVariable String username) {
    return ResponseEntity.ok(cartService.removeAll(username));
  }
}