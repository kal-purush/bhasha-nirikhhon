package ru.otus.hw.repositories;

import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import ru.otus.hw.models.Author;

import java.util.List;
import java.util.Set;


@RequiredArgsConstructor
public class AuthorRepositoryCustomImpl implements AuthorRepositoryCustom {

    private final MongoTemplate mongoTemplate;

    @Override
    public List<Author> findAllById(Set<String> authorIds) {
        Query query = new Query();
        query.addCriteria(Criteria.where("_id").in(authorIds));
        return mongoTemplate.find(query, Author.class);
    }
}