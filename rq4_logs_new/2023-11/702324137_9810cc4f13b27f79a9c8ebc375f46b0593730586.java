package pie.tomato.tomatomarket.infrastructure.persistence.wish;

import static pie.tomato.tomatomarket.domain.QCategory.*;
import static pie.tomato.tomatomarket.domain.QItem.*;
import static pie.tomato.tomatomarket.domain.QWish.*;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.querydsl.core.types.Projections;
import com.querydsl.jpa.impl.JPAQueryFactory;

import lombok.RequiredArgsConstructor;
import pie.tomato.tomatomarket.presentation.response.wish.CategoryWishListResponse;
import pie.tomato.tomatomarket.presentation.support.Principal;

@Repository
@RequiredArgsConstructor
public class WishCategoryRepository {

	private final JPAQueryFactory queryFactory;
	private final WishRepository wishRepository;

	public List<CategoryWishListResponse> findCategoryWishList(Principal principal) {
		return queryFactory.select(Projections.fields(CategoryWishListResponse.class,
				category.id.as("categoryId"),
				category.name.as("categoryName")))
			.from(wish)
			.innerJoin(wish.item, item)
			.innerJoin(item.category, category)
			.where(wishRepository.equalMemberId(principal.getMemberId()))
			.orderBy(category.id.desc())
			.fetch();
	}
}