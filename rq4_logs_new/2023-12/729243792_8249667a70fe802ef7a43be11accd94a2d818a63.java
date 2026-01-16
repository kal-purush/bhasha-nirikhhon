package com.chwipoClova.interview.entity;

import static com.querydsl.core.types.PathMetadataFactory.*;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.PathMetadata;
import javax.annotation.processing.Generated;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.PathInits;


/**
 * QInterview is a Querydsl query type for Interview
 */
@Generated("com.querydsl.codegen.DefaultEntitySerializer")
public class QInterview extends EntityPathBase<Interview> {

    private static final long serialVersionUID = 1729798915L;

    private static final PathInits INITS = PathInits.DIRECT2;

    public static final QInterview interview = new QInterview("interview");

    public final NumberPath<Long> interviewId = createNumber("interviewId", Long.class);

    public final DateTimePath<java.util.Date> modifyDate = createDateTime("modifyDate", java.util.Date.class);

    public final StringPath recruitSummary = createString("recruitSummary");

    public final DateTimePath<java.util.Date> regDate = createDateTime("regDate", java.util.Date.class);

    public final StringPath resumeSummary = createString("resumeSummary");

    public final StringPath title = createString("title");

    public final com.chwipoClova.user.entity.QUser user;

    public QInterview(String variable) {
        this(Interview.class, forVariable(variable), INITS);
    }

    public QInterview(Path<? extends Interview> path) {
        this(path.getType(), path.getMetadata(), PathInits.getFor(path.getMetadata(), INITS));
    }

    public QInterview(PathMetadata metadata) {
        this(metadata, PathInits.getFor(metadata, INITS));
    }

    public QInterview(PathMetadata metadata, PathInits inits) {
        this(Interview.class, metadata, inits);
    }

    public QInterview(Class<? extends Interview> type, PathMetadata metadata, PathInits inits) {
        super(type, metadata, inits);
        this.user = inits.isInitialized("user") ? new com.chwipoClova.user.entity.QUser(forProperty("user")) : null;
    }

}
