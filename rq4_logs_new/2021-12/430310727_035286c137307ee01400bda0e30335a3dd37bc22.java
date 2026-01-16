package ru.otus.spring.repositories.jpa;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import ru.otus.spring.domain.Author;
import ru.otus.spring.repositories.AuthorRepository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@Repository
public class AuthorRepositoryJpa implements AuthorRepository {

    @PersistenceContext
    private final EntityManager em;

    @Override
    public List<Author> getAuthors() {


        return em.createQuery("select a from Author a", Author.class).getResultList();
    }

    @Override
    public Optional<Author> getAuthorById(long id) {
       return Optional.ofNullable(em.find(Author.class,id));
    }
}