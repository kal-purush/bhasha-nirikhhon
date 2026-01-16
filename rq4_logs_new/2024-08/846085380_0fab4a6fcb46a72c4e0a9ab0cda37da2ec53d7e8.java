package vn.hust.hedspi.ezsport.controllers;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import vn.hust.hedspi.ezsport.dtos.feed.CreateFeedRequest;
import vn.hust.hedspi.ezsport.dtos.feed.UpdateFeedRequest;
import vn.hust.hedspi.ezsport.entities.Feed;
import vn.hust.hedspi.ezsport.entities.User;
import vn.hust.hedspi.ezsport.repositories.FeedRepository;
import vn.hust.hedspi.ezsport.repositories.UserRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RestController
@AllArgsConstructor
@NoArgsConstructor
@RequestMapping("api/v1/feed")
public class FeedController {
    @Autowired
    private FeedRepository feedRepository;

    @Autowired
    private UserRepository userRepository;

    //Create
    @GetMapping()
    public ResponseEntity<List<Feed>> getAllFeeds(){
        return ResponseEntity.ok(feedRepository.findAll());
    }

    //Create
    @PostMapping()
    public ResponseEntity<Feed> createFeed(@Valid @RequestBody CreateFeedRequest requestBody){
        User user = userRepository.findById(requestBody.getUserId()).orElse(null);
        if (user == null){
            return ResponseEntity.notFound().build();
        }
        if (requestBody.getStart().isAfter(requestBody.getEnd())){
            return ResponseEntity.badRequest().build();
        }

        Feed feed = new Feed();
        feed.setUser(user);
        feed.setDescription(requestBody.getDescription());
        feed.setStart(requestBody.getStart());
        feed.setEnd(requestBody.getEnd());
        feed.setDate(requestBody.getDate());
        feed.setLatitude(requestBody.getLatitude());
        feed.setLongitude(requestBody.getLongitude());
        feed.setBlock(requestBody.getBlock());

        return ResponseEntity.ok(feedRepository.save(feed));
    }

    //Show
    @GetMapping("/{id}")
    public ResponseEntity<Feed> getFeedById(@PathVariable UUID id){
        Optional<Feed> feed = feedRepository.findById(id);

        return feed.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    //Update
    @PutMapping("/{id}")
    public ResponseEntity<Feed> updateFeed(@PathVariable UUID id, @Valid @RequestBody UpdateFeedRequest requestBody){
        Optional<Feed> feed = feedRepository.findById(id);
        if (feed.isEmpty()){
            return ResponseEntity.notFound().build();
        }
        if (requestBody.getStart().isAfter(requestBody.getEnd())){
            return ResponseEntity.badRequest().build();
        }
        User user = userRepository.findById(requestBody.getUserId()).orElse(null);
        if (user == null){
            return ResponseEntity.notFound().build();
        }

        Feed feedToUpdate = feed.get();
        feedToUpdate.setUser(user);
        feedToUpdate.setDescription(requestBody.getDescription());
        feedToUpdate.setStart(requestBody.getStart());
        feedToUpdate.setEnd(requestBody.getEnd());
        feedToUpdate.setDate(requestBody.getDate());
        feedToUpdate.setStatus(requestBody.getStatus());
        feedToUpdate.setLatitude(requestBody.getLatitude());
        feedToUpdate.setLongitude(requestBody.getLongitude());
        feedToUpdate.setBlock(requestBody.getBlock());

        return ResponseEntity.ok(feedRepository.save(feedToUpdate));
    }

    //Delete
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteFeed(@PathVariable UUID id){
        Optional<Feed> feed = feedRepository.findById(id);
        if (feed.isEmpty()){
            return ResponseEntity.notFound().build();
        }

        feedRepository.delete(feed.get());
        return ResponseEntity.noContent().build();
    }
}