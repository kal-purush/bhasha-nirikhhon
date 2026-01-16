package refooding.api.common.qeurydsl;

import com.querydsl.jpa.impl.JPAQuery;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class QuerydslRepositoryUtils {

    private static final int ADDITIONAL_FETCH_COUNT = 1;

    public static <T> Slice<T> fetchSlice(JPAQuery<T> query, Pageable pageable) {

        List<T> content = query
                .limit(pageable.getPageSize() + ADDITIONAL_FETCH_COUNT)
                .fetch();

        if (content.size() > pageable.getPageSize()) {
            content.remove(pageable.getPageSize());
            return new SliceImpl<>(content, pageable, true);
        }

        return new SliceImpl<>(content, pageable, false);
    }

}