package com.artiexh.api.service.product;

import com.artiexh.model.domain.Product;
import com.artiexh.model.domain.ProductSuggestion;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.query.Query;

public interface OpenSearchProductService {
	Page<ProductSuggestion> getSuggestionInPage(Query query, Pageable pageable);

	Page<Product> getInPage(Query query, Pageable pageable);

	void save(Product product);

	void update(Product product);

	void delete(long productId);

}