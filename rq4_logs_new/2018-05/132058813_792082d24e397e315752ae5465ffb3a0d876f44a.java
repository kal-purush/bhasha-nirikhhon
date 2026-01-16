package by.ras.repository;

import by.ras.entity.particular.Order;
import by.ras.entity.particular.User;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;

@Repository
@Transactional
public interface OrderPagingRepository extends PagingAndSortingRepository<Order, Long> {

    Page<Order> findByUser(User user, Pageable pageable);
}