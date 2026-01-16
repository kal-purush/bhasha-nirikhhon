package com.example.team258.domain.bookSearch.repository;

import com.example.team258.common.entity.Book;
import com.jayway.jsonpath.Criteria;
import lombok.RequiredArgsConstructor;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Component
public class CustomBookSearchRepositoryImpl implements CustomBookSearchRepository {

    private final ElasticsearchOperations elasticsearchOperations;

    @Override
    public List<Book> searchByName(String book_name, Pageable pageable) {
        Criteria criteria = Criteria.where("basicProfile.name").contains(name);
        Query query = new CriteriaQuery(criteria).setPageable(pageable);
        SearchHits<User> search = elasticsearchOperations.search(query, User.class);
        return search.stream()
            .map(SearchHit::getContent)
            .collect(Collectors.toList());
    }
}