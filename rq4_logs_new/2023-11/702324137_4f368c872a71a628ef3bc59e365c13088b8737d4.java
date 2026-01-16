package pie.tomato.tomatomarket.infrastructure.persistence.memberTown;

import static pie.tomato.tomatomarket.domain.QRegion.*;

import java.util.List;

import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Repository;

import com.querydsl.core.types.Projections;
import com.querydsl.jpa.impl.JPAQueryFactory;

import lombok.RequiredArgsConstructor;
import pie.tomato.tomatomarket.infrastructure.persistence.region.RegionRepository;
import pie.tomato.tomatomarket.infrastructure.persistence.util.PaginationUtil;
import pie.tomato.tomatomarket.presentation.memberTown.response.MemberTownListResponse;

@Repository
@RequiredArgsConstructor
public class MemberTownPaginationRepository {

	private final JPAQueryFactory queryFactory;
	private final RegionRepository regionRepository;

	public Slice<MemberTownListResponse> findByAddressName(String addressName, int size, Long addressId) {
		List<MemberTownListResponse> responses = queryFactory.select(Projections.fields(MemberTownListResponse.class,
				region.id.as("addressId"),
				region.fullAddressName,
				region.addressName))
			.from(region)
			.where(
				regionRepository.lessThanAddressId(addressId),
				regionRepository.equalsAddressName(addressName))
			.orderBy(region.fullAddressName.desc())
			.limit(size + 1)
			.fetch();
		return PaginationUtil.checkLastPage(size, responses);
	}
}