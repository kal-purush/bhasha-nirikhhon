package com.solvd.elasticsearchmicroservice.domain.criteria.field;

import org.springframework.data.elasticsearch.core.query.CriteriaQuery;

public interface Field {

    void apply(CriteriaQuery query);

}