package dev.frilly.locket.controller;

import com.cloudinary.Cloudinary;
import dev.frilly.locket.data.Post;
import dev.frilly.locket.data.User;
import dev.frilly.locket.dto.req.IdRequest;
import dev.frilly.locket.dto.res.PaginatedResponse;
import dev.frilly.locket.dto.res.PostResponse;
import dev.frilly.locket.repo.PostRepository;
import dev.frilly.locket.repo.UserRepository;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Controller for handling posts-related tasks
 */
@RestController
public final class PostsController {

  @Autowired
  private PostRepository postRepo;

  @Autowired
  private Cloudinary cloudinary;

  @Autowired
  private UserRepository userRepo;

  /**
   * GET /posts
   * <p>
   * Retrieve viewable posts within a certain time frame.
   */
  @GetMapping("/posts")
  public PaginatedResponse<PostResponse> getPosts(
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(value = "per_page", defaultValue = "20") int perPage,
      @RequestParam LocalDateTime since,
      @RequestParam(required = false) LocalDateTime until
  ) {
    final var auth      = SecurityContextHolder.getContext()
        .getAuthentication();
    final var user      = (User) auth.getPrincipal();
    final var untilTime = until == null ? LocalDateTime.now() : until;

    final var query = postRepo.getPostsInRange(user, since, untilTime,
        PageRequest.of(page, perPage, Sort.by("time")));

    return new PaginatedResponse<>(query.getTotalElements(),
        query.getTotalPages(), page, perPage,
        query.getContent().stream().map(Post::makeResponse).toList());
  }

  /**
   * POST /posts
   * <p>
   * Upload a new post.
   */
  @PostMapping(value = "/posts", consumes = {"multipart/form-data"})
  public PostResponse postPost(
      @RequestPart MultipartFile image,
      @RequestPart String message,
      @RequestPart String viewers
  ) {
    final var auth = SecurityContextHolder.getContext().getAuthentication();
    final var user = (User) auth.getPrincipal();

    final var viewersSet = Arrays.stream(viewers.split(",")).map(it -> {
      long id = -1;
      try {
        id = Long.parseLong(it);
      } catch (NumberFormatException ex) {
      }
      return id;
    }).collect(Collectors.toSet());
    final var query = userRepo.findByIds(viewersSet);

    final var url = uploadImage(image);
    final var obj = new Post(user, url, message);
    obj.viewers().addAll(query);
    return postRepo.save(obj).makeResponse();
  }

  private String uploadImage(final MultipartFile image) throws
                                                        ResponseStatusException {
    if (image.getSize() > 10 * 1024 * 1024) {
      throw new ResponseStatusException(HttpStatus.REQUEST_ENTITY_TOO_LARGE);
    }

    final var allowed = List.of("image/webp", "image/jpeg", "image/png");
    if (!allowed.contains(image.getContentType())) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
    }

    try {
      final var res = cloudinary.uploader().upload(image.getBytes(), Map.of());
      return (String) res.get("url");
    } catch (IOException ex) {
      ex.printStackTrace();
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @PutMapping(value = "/posts", consumes = {"multipart/form-data"})
  public PostResponse putPost(
      @RequestPart long id,
      @RequestPart(required = false) MultipartFile image,
      @RequestPart(required = false) String message,
      @RequestPart(required = false) String viewers
  ) {
    final var auth = SecurityContextHolder.getContext().getAuthentication();
    final var user = (User) auth.getPrincipal();

    final var post = postRepo.findById(id);
    if (post.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    }
    if (post.get().user().id() != user.id()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }

    if (image != null) {
      final var url = uploadImage(image);
      post.get().setImageLink(url);
    }

    if (message != null) {
      post.get().setMessage(message);
    }

    if (viewers != null) {
      final var viewersSet = Arrays.stream(viewers.split(",")).map(it -> {
        long id1 = -1;
        try {
          id1 = Long.parseLong(it);
        } catch (NumberFormatException ex) {
        }
        return id1;
      }).collect(Collectors.toSet());
      final var query = userRepo.findByIds(viewersSet);

      post.get().viewers().clear();
      post.get().viewers().addAll(query);
    }

    return postRepo.save(post.get()).makeResponse();
  }

  /**
   * DELETE /posts.
   */
  @DeleteMapping("/posts")
  public PostResponse deletePosts(@Valid @RequestBody final IdRequest body) {
    final var auth = SecurityContextHolder.getContext().getAuthentication();
    final var user = (User) auth.getPrincipal();

    final var post = postRepo.findById(body.id());
    if (post.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    }
    if (post.get().user().id() != user.id()) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }

    postRepo.delete(post.get());
    return post.get().makeResponse();
  }

}