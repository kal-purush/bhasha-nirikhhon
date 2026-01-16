package com.chwipoClova.tmp.entity;

import static com.querydsl.core.types.PathMetadataFactory.*;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.PathMetadata;
import javax.annotation.processing.Generated;
import com.querydsl.core.types.Path;


/**
 * QTmp is a Querydsl query type for Tmp
 */
@Generated("com.querydsl.codegen.DefaultEntitySerializer")
public class QTmp extends EntityPathBase<Tmp> {

    private static final long serialVersionUID = 1739213507L;

    public static final QTmp tmp = new QTmp("tmp");

    public final NumberPath<Long> fld01 = createNumber("fld01", Long.class);

    public final StringPath fld02 = createString("fld02");

    public final StringPath fld03 = createString("fld03");

    public final StringPath fld04 = createString("fld04");

    public final StringPath fld05 = createString("fld05");

    public QTmp(String variable) {
        super(Tmp.class, forVariable(variable));
    }

    public QTmp(Path<? extends Tmp> path) {
        super(path.getType(), path.getMetadata());
    }

    public QTmp(PathMetadata metadata) {
        super(Tmp.class, metadata);
    }

}
