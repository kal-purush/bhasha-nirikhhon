package com.customs.network.fdapn.initializers;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PostgresFunctionInit {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public PostgresFunctionInit(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    public void scanSchemaFunctionInit(){
        String functionSql = "CREATE OR REPLACE FUNCTION fetch_data_by_col_value(fieldName VARCHAR, p_value VARCHAR, p_schemaName VARCHAR) " +
                "RETURNS TABLE ( " +
                "    serial BIGINT, " +
                "    batch_id VARCHAR(255), " +
                "    account_id VARCHAR(255), " +
                "    created_on TIMESTAMP WITHOUT TIME ZONE, " +
                "    envelop_number VARCHAR(255), " +
                "    reference_id VARCHAR(255), " +
                "    request_json JSONB, " +
                "    response_json JSONB, " +
                "    status VARCHAR(255), " +
                "    trace_id VARCHAR(255), " +
                "    updated_on DATE, " +
                "    user_id VARCHAR(255) " +
                ") " +
                "AS $$ " +
                "DECLARE " +
                "    table_name_var TEXT; " +
                "    table_schema_var TEXT := p_schemaName;  " +
                "BEGIN " +
                "    FOR table_name_var IN ( " +
                "        SELECT table_name " +
                "        FROM information_schema.tables " +
                "        WHERE table_schema = table_schema_var " +
                "        AND table_type = 'BASE TABLE' " +
                "        AND table_name !~ '.*_[0-9]+$' " +
                "    ) " +
                "    LOOP " +
                "        RAISE NOTICE 'Processing table: %', table_name_var; " +
                "        RETURN QUERY EXECUTE " +
                "            'SELECT DISTINCT * FROM ' || table_schema_var || '.' || table_name_var || " +
                "            ' WHERE ' || quote_ident(fieldName) || ' = $1' " +
                "        USING p_value; " +
                "    END LOOP; " +
                "END; " +
                "$$ LANGUAGE plpgsql;";

        jdbcTemplate.execute(functionSql);
    }
}