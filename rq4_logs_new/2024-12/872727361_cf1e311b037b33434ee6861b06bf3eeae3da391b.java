package io.d4rkr0n1n.backend.clients;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import io.d4rkr0n1n.backend.model.Note;

@FeignClient(name = "database", url = "http://localhost:8082/api/v1")
public interface DatabaseClient {
  @GetMapping("retrieveAllNotes")
  List<Note> retrieveAllNotes();

  @GetMapping("retrieveNote")
  Optional<Note> retrieveNote(@RequestParam UUID id);

  @PostMapping("saveNote")
  Note saveNote(@RequestBody Note note);

  @GetMapping("countNotes")
  Long countNotes();

  @DeleteMapping("deleteNote")
  String deleteNote(@RequestBody Note note);
}