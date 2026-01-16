package org.example.canon.specification;

import org.example.canon.entity.Post;
import org.springframework.data.jpa.domain.Specification;

import java.util.Date;

public class PostSpecification {

//    public String category;
//    public String tool;
//    public String major;
//    public Date year;

    public static Specification<Post> filterByCategory(String category) {
        if (category != null && !category.isEmpty()) {
            return (root, query, criteriaBuilder) ->
                    criteriaBuilder.equal(root.get("category"), category);
        } return Specification.where(null);
    }
    public static Specification<Post> filterByTool(String tool) {
        if (tool != null && !tool.isEmpty()) {
            return (root, query, criteriaBuilder) ->
                    criteriaBuilder.equal(root.get("tool"), tool);
        } return Specification.where(null);
    }

    public static Specification<Post> filterByMajor(String major) {
        if (major != null && !major.isEmpty()) {
            return (root, query, criteriaBuilder) ->
                    criteriaBuilder.equal(root.get("major"), major);
        } return Specification.where(null);
    }

    public static Specification<Post> filterByYear(String year) {
        if (year != null && !year.isEmpty()) {
            return (root, query, criteriaBuilder) ->
                    criteriaBuilder.equal(root.get("year"), year);
        } return Specification.where(null);
    }


}