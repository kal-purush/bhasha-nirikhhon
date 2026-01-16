/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.db.fieldmask.CrawlExecutionQueryBuilder;
import no.nb.nna.veidemann.db.fieldmask.JobExecutionQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rethinkdb.RethinkDB.r;

public class ListCrawlExecutionQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ListCrawlExecutionQueryBuilder.class);
    private static final CrawlExecutionQueryBuilder NO_MASK_BUILDER = new CrawlExecutionQueryBuilder();

    private ReqlExpr q;
    private final CrawlExecutionsListRequest request;
    final Tables table;

    public ListCrawlExecutionQueryBuilder(CrawlExecutionsListRequest request) {
        this.request = request;
        table = Tables.EXECUTIONS;

        q = r.table(table.name);

        if (!request.getOrderByPath().isEmpty()) {
            if (request.getOrderDescending()) {
                q = q.orderBy().optArg("index",
                        r.desc(NO_MASK_BUILDER.getSortIndexForPath(request.getOrderByPath())));
            } else {
                q = q.orderBy().optArg("index",
                        r.asc(NO_MASK_BUILDER.getSortIndexForPath(request.getOrderByPath())));
            }
        }

        if (request.getIdCount() > 0) {
            if (q instanceof Table) {
                q = ((Table) q).getAll(request.getIdList().toArray());
            } else {
                q = q.filter(row -> r.expr(request.getIdList().toArray()).contains(row.g("id")));
            }
        }

        if (request.hasStartTimeFrom() || request.hasStartTimeTo()) {
            buildStartTimeFilter();
        }

        if (request.getStateCount() > 0) {
            if (q instanceof Table) {
                q = ((Table) q).getAll(request.getStateList().toArray()).optArg("index", "state");
            } else {
                q = q.filter(row -> r.expr(request.getStateList().toArray()).contains(row.g("state")));
            }
        }

        if (request.getHasError()) {
            q = q.filter(doc -> doc.hasFields("error"));
        }

        if (request.hasQueryTemplate() && request.hasQueryMask()) {
            CrawlExecutionQueryBuilder queryBuilder = new CrawlExecutionQueryBuilder(request.getQueryMask());
            q = q.filter(queryBuilder.buildFilterQuery(request.getQueryTemplate()));
        }
    }

    public ReqlExpr getListQuery() {
        ReqlExpr query = q;

        if (request.hasReturnedFieldsMask()) {
            CrawlExecutionQueryBuilder queryBuilder = new CrawlExecutionQueryBuilder(request.getReturnedFieldsMask());
            query = query.pluck(queryBuilder.createPluckQuery());
        }

        if (request.getPageSize() > 0 || request.getOffset() > 0) {
            query = query.skip(request.getOffset()).limit(request.getPageSize());
        }

        return query;
    }

    public ReqlExpr getCountQuery() {
        return q.count();
    }

    void buildStartTimeFilter() {
        Object from = r.minval();
        Object to = r.maxval();

        if (request.hasStartTimeFrom()) {
            from = ProtoUtils.tsToOdt(request.getStartTimeFrom());
        }
        if (request.hasStartTimeTo()) {
            to = ProtoUtils.tsToOdt(request.getStartTimeTo());
        }

        if (q instanceof Table) {
            q = ((Table) q).between(from, to).optArg("index", "startTime");
        } else {
            Object f = from;
            Object t = to;
            q = q.filter(row -> row.g("startTime").ge(f).and(row.g("startId").lt(t)));
        }
    }
}