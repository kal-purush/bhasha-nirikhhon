package db.migration;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class V3__insert_test_data_into_customers extends BaseJavaMigration {

    @Override
    public void migrate(Context context){
        new JdbcTemplate(new SingleConnectionDataSource(context.getConnection(), true))
                .execute("INSERT INTO customer (name, last_name, email, phone_number) " +
                        "VALUES ('Janusz', 'Paruwnik', 'janusz@gmail.com', '789789789')");
    }
}