package com.openpromt.coffeee.swf2023.openpromtserver.copyright.entity;

import static com.querydsl.core.types.PathMetadataFactory.*;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.PathMetadata;
import javax.annotation.processing.Generated;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.PathInits;


/**
 * QCopyright is a Querydsl query type for Copyright
 */
@Generated("com.querydsl.codegen.DefaultEntitySerializer")
public class QCopyright extends EntityPathBase<Copyright> {

    private static final long serialVersionUID = -1321095273L;

    private static final PathInits INITS = PathInits.DIRECT2;

    public static final QCopyright copyright = new QCopyright("copyright");

    public final com.openpromt.coffeee.swf2023.openpromtserver.util.auditing.QBaseEntity _super = new com.openpromt.coffeee.swf2023.openpromtserver.util.auditing.QBaseEntity(this);

    public final StringPath copyright_title = createString("copyright_title");

    public final NumberPath<Long> copyrightId = createNumber("copyrightId", Long.class);

    //inherited
    public final DateTimePath<java.time.LocalDateTime> CRE_DTTM = _super.CRE_DTTM;

    //inherited
    public final NumberPath<Long> CRE_ID = _super.CRE_ID;

    public final StringPath privKey = createString("privKey");

    public final StringPath pubKey = createString("pubKey");

    //inherited
    public final DateTimePath<java.time.LocalDateTime> UPD_DTTM = _super.UPD_DTTM;

    //inherited
    public final NumberPath<Long> UPD_ID = _super.UPD_ID;

    public final com.openpromt.coffeee.swf2023.openpromtserver.user.entity.QUser user;

    public QCopyright(String variable) {
        this(Copyright.class, forVariable(variable), INITS);
    }

    public QCopyright(Path<? extends Copyright> path) {
        this(path.getType(), path.getMetadata(), PathInits.getFor(path.getMetadata(), INITS));
    }

    public QCopyright(PathMetadata metadata) {
        this(metadata, PathInits.getFor(metadata, INITS));
    }

    public QCopyright(PathMetadata metadata, PathInits inits) {
        this(Copyright.class, metadata, inits);
    }

    public QCopyright(Class<? extends Copyright> type, PathMetadata metadata, PathInits inits) {
        super(type, metadata, inits);
        this.user = inits.isInitialized("user") ? new com.openpromt.coffeee.swf2023.openpromtserver.user.entity.QUser(forProperty("user")) : null;
    }

}
