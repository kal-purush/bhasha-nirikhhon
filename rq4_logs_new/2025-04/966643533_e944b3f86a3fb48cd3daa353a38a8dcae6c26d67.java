package org.spiceboys.Travel.Diary.controller;

import org.spiceboys.Travel.Diary.model.Favourite;
import org.spiceboys.Travel.Diary.service.FavouriteService;
import org.spiceboys.Travel.Diary.service.UserService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class FavouriteController {
    private final FavouriteService favouriteService;

    public FavouriteController(FavouriteService favouriteService, UserService userService) {
        this.favouriteService = favouriteService;
    }

    // GET /api/users/user_id/{userId}/favourites

    // POST /api/favourites
    @PostMapping("/api/favourites")
    public ResponseEntity<Favourite> saveFavourite(@RequestBody Favourite favourite) {
        Favourite savedFavourite = favouriteService.saveFavourite(favourite);
        return ResponseEntity.status(HttpStatus.CREATED).body(savedFavourite);
    }
}