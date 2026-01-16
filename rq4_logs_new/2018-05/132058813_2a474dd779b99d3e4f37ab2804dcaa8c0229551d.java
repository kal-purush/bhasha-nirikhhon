package by.ras.repository.specification;

import by.ras.entity.particular.Product;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;


public class ProductSpecification {

    public static Specification<Product> searchProducts(Product filter) {
        return new Specification<Product>() {
            @Override
            public Predicate toPredicate(Root<Product> root, CriteriaQuery<?> query, CriteriaBuilder cb) {

                List<Predicate> predicates = new ArrayList<>();

                // If designation is specified in filter, add equal where clause
                if (filter.getCompany() != null) {
                    predicates.add(cb.equal(root.get("company"), filter.getCompany()));
                }
//                if (filter.getProductName() != null) {
//                    predicates.add(cb.equal(root.get("product_name"), filter.getProductName()));
//                }
                if (filter.getModel() != null) {
                    predicates.add(cb.equal(root.get("model"), filter.getModel()));
                }
//                if (filter.getProductType() != null) {
//                    predicates.add(cb.equal(root.get("product_type"), filter.getProductType()));
//                }
                if (filter.getPrice() != null) {
                    predicates.add(cb.equal(root.get("price"), filter.getPrice()));
                }

                return cb.and(predicates.toArray(new Predicate[0]));
            }
        };

    }
}
