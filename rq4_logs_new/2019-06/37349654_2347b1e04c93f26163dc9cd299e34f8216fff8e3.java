package org.zalando.compass.domain.logic.revision;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.zalando.compass.domain.event.DimensionCreated;
import org.zalando.compass.domain.event.DimensionDeleted;
import org.zalando.compass.domain.event.DimensionReplaced;
import org.zalando.compass.domain.model.Dimension;
import org.zalando.compass.domain.model.revision.DimensionRevision;
import org.zalando.compass.domain.model.Revision;
import org.zalando.compass.domain.repository.revision.DimensionRevisionRepository;
import org.zalando.compass.infrastructure.database.model.enums.RevisionType;

import javax.annotation.Nullable;

import static org.zalando.compass.infrastructure.database.model.enums.RevisionType.CREATE;
import static org.zalando.compass.infrastructure.database.model.enums.RevisionType.DELETE;
import static org.zalando.compass.infrastructure.database.model.enums.RevisionType.UPDATE;

@Slf4j
@Component
@AllArgsConstructor(onConstructor = @__(@Autowired))
class DimensionRevisioning {

    private final DimensionRevisionRepository repository;

    @EventListener
    public void onDimensionCreated(final DimensionCreated event) {
        final Dimension dimension = event.getDimension();
        final Revision revision = event.getRevision();

        createRevision(dimension, revision, CREATE);
    }

    @EventListener
    public void onDimensionReplaced(final DimensionReplaced event) {
        final Revision revision = event.getRevision();

        @Nullable final Dimension before = event.getBefore();
        final Dimension after = event.getAfter();

        if (before == null) {
            createRevision(after, revision, CREATE);
        } else {
            createRevision(after, revision, UPDATE);
        }
    }

    @EventListener
    public void ondDimensionDeleted(final DimensionDeleted event) {
        final Revision revision = event.getRevision();
        final Dimension dimension = event.getDimension();

        createRevision(dimension, revision, DELETE);
    }

    private void createRevision(final Dimension dimension, final Revision revision, final RevisionType update) {
        final DimensionRevision dimensionRevision = dimension.toRevision(revision.withType(update));
        repository.create(dimensionRevision);
        log.info("Created dimension revision [{}]", dimensionRevision);
    }

}