package io.d4rkr0n1n.database.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.d4rkr0n1n.database.model.Note;

@RepositoryRestResource(collectionResourceRel = "notes", path = "notes")
public interface NotesRepository extends PagingAndSortingRepository<Note, UUID> {
    List<Note> findByName(@Param("name") String name);
}