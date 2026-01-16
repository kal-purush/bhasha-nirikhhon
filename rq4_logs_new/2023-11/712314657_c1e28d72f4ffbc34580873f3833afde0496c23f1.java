package az.ingress.bookstore.dao.repo;

import az.ingress.bookstore.dao.entity.Subscriber;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SubscriberRepository extends JpaRepository<Subscriber, Long> {
    List<Object[]> findStudentAndAuthorDetails(@Param("authorId") Long authorId);
    List<Object[]> findStudentAndAuthorDetail(@Param("studentId") Long studentId);

}