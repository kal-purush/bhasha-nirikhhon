package ru.aaxee.homework2.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import ru.aaxee.homework2.domain.Book;
import ru.aaxee.homework2.domain.Comment;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ComponentScan
class CommentJpaRepositoryTest {

    @PersistenceContext
    private EntityManager em;

    @Autowired
    private CommentRepository commentRepository;

    @Test
    void addComment_findByBookId() {
        // given
        String text = "Хорошая книга, и комменатрии интересные";
        Book book = new Book();
        em.persist(book);
        Comment comment = new Comment(text);
        comment.setBook(book);
        em.persist(comment);

        // when
        List<Comment> found = commentRepository.findAllByBookId(1L);

        // then
        assertThat(getOnlyElement(found).getText()).isEqualTo(text);
    }
}