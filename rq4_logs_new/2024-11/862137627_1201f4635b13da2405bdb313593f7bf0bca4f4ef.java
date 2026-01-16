package org.example.backend.controllers.client.gioHang;

import lombok.RequiredArgsConstructor;

import org.example.backend.dto.request.gioHang.GioHangChiTietRequest;
import org.example.backend.dto.response.gioHang.GioHangChiTietResponse;
import org.example.backend.services.GioHangService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/cart")
@RequiredArgsConstructor
public class GioHangController {
    private final GioHangService gioHangService;

    @GetMapping("/{userId}")
    public ResponseEntity<List<GioHangChiTietResponse>> getGioHang(@PathVariable UUID userId) {
        return ResponseEntity.ok(gioHangService.getGioHang(userId));
    }

    @PostMapping("/{userId}/them-san-pham")
    public ResponseEntity<GioHangChiTietResponse> addToCart(
            @PathVariable UUID userId,
            @RequestBody GioHangChiTietRequest request) {
        return ResponseEntity.ok(gioHangService.addToCart(userId, request));
    }

    @DeleteMapping("/{userId}/xoa-san-pham/{gioHangChiTietId}")
public ResponseEntity<Void> deleteFromCart(
        @PathVariable UUID userId,
        @PathVariable UUID gioHangChiTietId) {
    gioHangService.deleteFromCart(userId, gioHangChiTietId);
    return ResponseEntity.ok().build();
}
}