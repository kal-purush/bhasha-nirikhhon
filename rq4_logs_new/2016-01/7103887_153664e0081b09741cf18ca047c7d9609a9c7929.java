package io.sinq.ext;

import org.hibernate.dialect.PostgreSQL9Dialect;

import java.sql.Types;

public class PostgresExDialect extends PostgreSQL9Dialect {
    public PostgresExDialect() {
        registerColumnType(Types.OTHER, "inet");
    }
}