package ru.otus.spring.repositories;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import ru.otus.spring.domain.Book;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class BookRepositoryCustomImpl implements BookRepositoryCustom {
    @PersistenceContext
    private final EntityManager em;

    @Override
    public Optional<Book> findBookWith1inTitle() {
        return Optional.of(em.createQuery("select b from Book b " +
                "where b.title like '%1%'", Book.class).getSingleResult());
    }
}