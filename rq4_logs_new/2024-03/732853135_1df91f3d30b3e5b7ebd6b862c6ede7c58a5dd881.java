package com.example.parking.api.favorite;

import com.example.parking.application.favorite.FavoriteService;
import com.example.parking.application.favorite.dto.FavoriteCreateRequest;
import com.example.parking.application.favorite.dto.FavoriteDeleteRequest;
import com.example.parking.config.argumentresolver.MemberAuth;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
public class FavoriteController {

    private final FavoriteService favoriteService;

    @PostMapping("/favorites")
    public ResponseEntity<Void> create(@RequestBody FavoriteCreateRequest favoriteCreateRequest,
                                       @MemberAuth Long memberId) {
        favoriteService.createFavorite(favoriteCreateRequest, memberId);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping("/favorites")
    public ResponseEntity<Void> delete(@RequestBody FavoriteDeleteRequest favoriteDeleteRequest,
                                       @MemberAuth Long memberId) {
        favoriteService.deleteFavorite(favoriteDeleteRequest, memberId);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}