package JPABook.JPAShop.repository;

import JPABook.JPAShop.domain.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;

@Repository
public class OrderRepository {
    @Autowired private final EntityManager em;

    public OrderRepository(EntityManager em) {
        this.em = em;
    }

    public void save(Order order) {
        em.persist(order);
    }

    public Order findOne(Long id) {
        return em.find(Order.class, id);
    }

    //COMMENT: cancel 의 경우 도메인에서 엔티티를 건드려서 상태 값의 수정을 하기 때문에
    // repo 에서 db 를 직접 커넥하여 수정해줄 필요가 없다
}