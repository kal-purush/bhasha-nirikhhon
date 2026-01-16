package ru.aaxee.homework2.repository;

import lombok.extern.java.Log;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.aaxee.homework2.domain.Author;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;

@Log
@Repository
public class AuthorJpaRepository implements AuthorRepository {

    @PersistenceContext
    private EntityManager em;

    @Override
    @Transactional
    public void addAuthor(String name) {
        Author author = new Author();
        author.setName(name);
        em.persist(author);
        log.info(author.getId().toString());
    }

    @Override
    public Optional<Author> findById(Long id) {
        return Optional.ofNullable(em.find(Author.class, id));
    }

    @Override
    public List<Author> findAll() {
        return em.createQuery("select a from Author a", Author.class).getResultList();
    }

    @Override
    @Transactional
    public void updateAuthor(Long id, String name) {
        Author author = new Author();
        author.setId(id);
        author.setName(name);
        em.persist(author);
    }

    @Override
    @Transactional
    public void deleteById(Long id) {
        Author author = new Author();
        author.setId(id);
        em.remove(em.contains(author) ? author : em.merge(author));
    }

    @Override
    public Optional<Author> findByName(String name) {
        TypedQuery<Author> query = em
                .createQuery("select a from Author a where a.name = :name", Author.class)
                .setParameter("name", name);
        return Optional.ofNullable(getOnlyElement(query.getResultList(), null));
    }
}