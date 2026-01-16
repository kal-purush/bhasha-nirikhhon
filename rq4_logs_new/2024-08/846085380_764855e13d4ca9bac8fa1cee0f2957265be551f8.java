package vn.hust.hedspi.ezsport.controllers;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import vn.hust.hedspi.ezsport.dtos.playerBooking.CreatePlayerBookingRequest;
import vn.hust.hedspi.ezsport.dtos.playerBooking.UpdatePlayerBookingRequest;
import vn.hust.hedspi.ezsport.entities.Feed;
import vn.hust.hedspi.ezsport.entities.PlayerBooking;
import vn.hust.hedspi.ezsport.entities.User;
import vn.hust.hedspi.ezsport.repositories.FeedRepository;
import vn.hust.hedspi.ezsport.repositories.PlayerBookingRepository;
import vn.hust.hedspi.ezsport.repositories.UserRepository;

import java.util.List;
import java.util.UUID;

@RestController
@NoArgsConstructor
@AllArgsConstructor
@RequestMapping("api/v1/player-booking")
public class PlayerBookingController {
    @Autowired
    private PlayerBookingRepository playerBookingRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private FeedRepository feedRepository;

    //List all player booking
    @GetMapping()
    public ResponseEntity<List<PlayerBooking>> getAllPlayerBookings(){
        return ResponseEntity.ok(playerBookingRepository.findAll());
    }

    //Create
    @PostMapping()
    public ResponseEntity<PlayerBooking> createPlayerBooking(@Valid @RequestBody CreatePlayerBookingRequest requestBody) {
        User bookingPlayer = userRepository.findById(requestBody.getBookingPlayerId()).orElse(null);
        if (bookingPlayer == null) {
            return ResponseEntity.notFound().build();
        }

        User bookedPlayer = userRepository.findById(requestBody.getBookedPlayerId()).orElse(null);
        if (bookedPlayer == null) {
            return ResponseEntity.notFound().build();
        } else if (bookedPlayer.getId().equals(bookingPlayer.getId())) {
            return ResponseEntity.badRequest().build();

        }

        Feed feed = feedRepository.findById(requestBody.getFeedId()).orElse(null);
        if (feed == null) {
            return ResponseEntity.notFound().build();
        }

        PlayerBooking playerBooking = new PlayerBooking();
        playerBooking.setBookingUser(bookingPlayer);
        playerBooking.setBookedUser(bookedPlayer);
        playerBooking.setFeed(feed);

        return ResponseEntity.ok(playerBookingRepository.save(playerBooking));
    }

    //Show
    @GetMapping("/{id}")
    public ResponseEntity<PlayerBooking> getPlayerBookingById(@PathVariable UUID id){
        return playerBookingRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    //Update
    @PutMapping("/{id}")
    public ResponseEntity<PlayerBooking> updatePlayerBooking(@PathVariable UUID id, @Valid @RequestBody UpdatePlayerBookingRequest requestBody) {
        PlayerBooking playerBooking = playerBookingRepository.findById(id).orElse(null);
        if (playerBooking == null) {
            return ResponseEntity.notFound().build();
        }

        User bookingPlayer = userRepository.findById(requestBody.getBookingPlayerId()).orElse(null);
        if (bookingPlayer == null) {
            return ResponseEntity.notFound().build();
        }

        User bookedPlayer = userRepository.findById(requestBody.getBookedPlayerId()).orElse(null);
        if (bookedPlayer == null) {
            return ResponseEntity.notFound().build();
        } else if (bookedPlayer.getId().equals(bookingPlayer.getId())) {
            return ResponseEntity.badRequest().build();
        }

        Feed feed = feedRepository.findById(requestBody.getFeedId()).orElse(null);
        if (feed == null) {
            return ResponseEntity.notFound().build();
        }

        playerBooking.setBookingUser(bookingPlayer);
        playerBooking.setBookedUser(bookedPlayer);
        playerBooking.setFeed(feed);
        playerBooking.setStatus(requestBody.getStatus());

        return ResponseEntity.ok(playerBookingRepository.save(playerBooking));
    }

    //Delete
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deletePlayerBooking(@PathVariable UUID id){
        if (playerBookingRepository.existsById(id)){
            playerBookingRepository.deleteById(id);
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.notFound().build();
    }
}