package com.exasol.adapter.document;

import java.util.List;
import java.util.stream.Stream;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.document.documentfetcher.DocumentFetcher;
import com.exasol.adapter.document.mapping.PropertyToColumnValueExtractorFactory;
import com.exasol.adapter.document.mapping.SchemaMapper;
import com.exasol.adapter.document.queryplanning.RemoteTableQuery;
import com.exasol.sql.expression.ValueExpression;

/**
 * Abstract basis for dialect specific {@link DataLoader}s.
 */
@java.lang.SuppressWarnings("squid:S119") // DocumentVisitorType does not fit naming conventions.
public abstract class AbstractDataLoader<DocumentVisitorType> implements DataLoader {
    private static final long serialVersionUID = 8650673888516548639L;
    /** @serial */
    private final DocumentFetcher<DocumentVisitorType> documentFetcher;

    /**
     * Create a new instance of {@link AbstractDataLoader}.
     * 
     * @param documentFetcher document fetcher that provides the document data.
     */
    public AbstractDataLoader(final DocumentFetcher<DocumentVisitorType> documentFetcher) {
        this.documentFetcher = documentFetcher;
    }

    /**
     * Get a database specific {@link PropertyToColumnValueExtractorFactory}.
     *
     * @return database specific {@link PropertyToColumnValueExtractorFactory}
     */
    protected abstract PropertyToColumnValueExtractorFactory<DocumentVisitorType> getValueExtractorFactory();

    @Override
    public final Stream<List<ValueExpression>> run(final ExaConnectionInformation connectionInformation,
            final RemoteTableQuery remoteTableQuery) {
        final SchemaMapper<DocumentVisitorType> schemaMapper = new SchemaMapper<>(remoteTableQuery,
                getValueExtractorFactory());
        return this.documentFetcher.run(connectionInformation).flatMap(schemaMapper::mapRow);
    }
}