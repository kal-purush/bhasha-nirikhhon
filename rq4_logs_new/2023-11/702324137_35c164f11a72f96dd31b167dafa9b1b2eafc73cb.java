package pie.tomato.tomatomarket.application.category;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import pie.tomato.tomatomarket.domain.Category;
import pie.tomato.tomatomarket.infrastructure.persistence.category.CategoryRepository;
import pie.tomato.tomatomarket.presentation.category.response.CategoryListResponse;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CategoryService {

	private final CategoryRepository categoryRepository;

	public List<CategoryListResponse> findAll() {
		List<Category> categories = categoryRepository.findAll();
		return categories.stream()
			.map(CategoryListResponse::new)
			.collect(Collectors.toList());
	}
}