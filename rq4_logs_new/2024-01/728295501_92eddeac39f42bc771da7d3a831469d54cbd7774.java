package study.tipsyboy.tipsyboyMall.item.repository;

import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import study.tipsyboy.tipsyboyMall.item.domain.Item;
import study.tipsyboy.tipsyboyMall.item.domain.QItem;
import study.tipsyboy.tipsyboyMall.item.dto.ItemPagingRequestDto;

import java.util.List;

@RequiredArgsConstructor
public class ItemRepositoryCustomImpl implements ItemRepositoryCustom {

    private final JPAQueryFactory jpaQueryFactory;

    @Override
    public List<Item> getItems(ItemPagingRequestDto pagingRequestDto) {
        return jpaQueryFactory.selectFrom(QItem.item)
                .limit(pagingRequestDto.getSize())
                .offset(pagingRequestDto.getOffset())
                .orderBy(QItem.item.id.desc())
                .fetch();
    }
}