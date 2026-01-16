package org.zalando.compass.domain;

import org.zalando.compass.domain.model.Dimension;
import org.zalando.compass.domain.model.DimensionRevision;
import org.zalando.compass.domain.model.PageRevision;
import org.zalando.compass.domain.model.Revision;
import org.zalando.compass.library.pagination.PageResult;
import org.zalando.compass.library.pagination.Pagination;

public interface DimensionRevisionService {
    PageResult<Revision> readPageRevisions(Pagination<Long> query);

    PageRevision<Dimension> readPageAt(long revision, Pagination<String> query);

    PageResult<Revision> readRevisions(String id, Pagination<Long> query);

    DimensionRevision readAt(String id, long revision);
}