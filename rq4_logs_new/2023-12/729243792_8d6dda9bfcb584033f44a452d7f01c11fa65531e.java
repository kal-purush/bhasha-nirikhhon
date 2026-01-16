package com.chwipoClova.common.entity;

import static com.querydsl.core.types.PathMetadataFactory.*;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.PathMetadata;
import javax.annotation.processing.Generated;
import com.querydsl.core.types.Path;


/**
 * QApiLog is a Querydsl query type for ApiLog
 */
@Generated("com.querydsl.codegen.DefaultEntitySerializer")
public class QApiLog extends EntityPathBase<ApiLog> {

    private static final long serialVersionUID = -525634126L;

    public static final QApiLog apiLog = new QApiLog("apiLog");

    public final NumberPath<Long> apiLogId = createNumber("apiLogId", Long.class);

    public final StringPath apiUrl = createString("apiUrl");

    public final StringPath message = createString("message");

    public final DateTimePath<java.util.Date> regDate = createDateTime("regDate", java.util.Date.class);

    public final NumberPath<Long> userId = createNumber("userId", Long.class);

    public QApiLog(String variable) {
        super(ApiLog.class, forVariable(variable));
    }

    public QApiLog(Path<? extends ApiLog> path) {
        super(path.getType(), path.getMetadata());
    }

    public QApiLog(PathMetadata metadata) {
        super(ApiLog.class, metadata);
    }

}
