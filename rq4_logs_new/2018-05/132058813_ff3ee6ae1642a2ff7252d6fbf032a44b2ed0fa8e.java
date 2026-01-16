package by.ras.repository.specification;

import by.ras.entity.particular.Order;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;

public class OrderSpecification {
    public static Specification<Order> searchOrders(Order filter) {
        return new Specification<Order>() {
            @Override
            public Predicate toPredicate(Root<Order> root, CriteriaQuery<?> query, CriteriaBuilder cb) {

                List<Predicate> predicates = new ArrayList<>();

                // If designation is specified in filter, add equal where clause
                if (filter.getOrderStatus() != null) {
                    predicates.add(cb.equal(root.get("orderStatus"), filter.getOrderStatus()));
                }
                return cb.and(predicates.toArray(new Predicate[0]));
            }
        };

    }
}
