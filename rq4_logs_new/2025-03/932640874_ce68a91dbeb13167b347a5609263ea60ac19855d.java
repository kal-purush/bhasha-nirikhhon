package dev.frilly.locket.controller;

import dev.frilly.locket.data.Message;
import dev.frilly.locket.data.User;
import dev.frilly.locket.dto.req.IdRequest;
import dev.frilly.locket.dto.req.MessageRequest;
import dev.frilly.locket.dto.res.MessageResponse;
import dev.frilly.locket.dto.res.PaginatedResponse;
import dev.frilly.locket.repo.FriendshipRepository;
import dev.frilly.locket.repo.MessageRepository;
import dev.frilly.locket.repo.UserRepository;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Represents the controller that handles messages requests.
 */
@RestController
public final class MessagesController {

  @Autowired
  private UserRepository userRepo;

  @Autowired
  private MessageRepository messageRepo;

  @Autowired
  private FriendshipRepository friendshipRepo;

  /**
   * GET /messages.
   */
  public PaginatedResponse<MessageResponse> getMessages(
      @RequestParam(value = "page", defaultValue = "0") int page,
      @RequestParam(value = "per_page", defaultValue = "20") int perPage,
      @RequestParam(value = "since") LocalDateTime since,
      @RequestParam(value = "until", required = false) LocalDateTime until
  ) {
    final var auth = SecurityContextHolder.getContext().getAuthentication();
    final var user = (User) auth.getPrincipal();

    final var query = messageRepo.findMessages(user, since,
        Optional.ofNullable(until).orElse(LocalDateTime.now()),
        PageRequest.of(page, perPage, Sort.by(Direction.DESC, "time")));
    return new PaginatedResponse<>(query.getTotalElements(),
        query.getTotalPages(), page, perPage,
        query.get().map(Message::makeResponse).toList());
  }

  /**
   * POST /messages, for sending a message to another user.
   */
  @PostMapping("/messages")
  public MessageResponse postMessage(@Valid @RequestBody final MessageRequest body) {
    final var auth   = SecurityContextHolder.getContext().getAuthentication();
    final var user   = (User) auth.getPrincipal();
    final var target = userRepo.findById(body.receiver());

    if (target.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    }

    if (friendshipRepo.findWithTwoUsers(user, target.get()).isEmpty()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }

    final var msg = messageRepo.save(
        new Message(user, target.get(), body.content()));

    // TODO: Send a push notification.

    return msg.makeResponse();
  }

  /**
   * DELETE /messages.
   */
  @DeleteMapping("/messages")
  public MessageResponse deleteMessage(@RequestBody final IdRequest body) {
    final var auth = SecurityContextHolder.getContext().getAuthentication();
    final var user = (User) auth.getPrincipal();

    final var msg = messageRepo.findById(body.id());

    if (msg.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.NO_CONTENT);
    }

    if (msg.get().sender().id() != user.id()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }

    // TODO: Send a notification to devices to delete that.

    messageRepo.delete(msg.get());
    return msg.get().makeResponse();
  }

}