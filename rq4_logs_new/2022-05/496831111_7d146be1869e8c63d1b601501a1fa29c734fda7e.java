package rifqimuhammadaziz.springdatarest.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import rifqimuhammadaziz.springdatarest.entity.Book;

public interface BookRepository extends PagingAndSortingRepository<Book, Long> {

    Book findBookByTitle(String title);

    Page<Book> findBookByAuthor(String author, Pageable pageable);
}