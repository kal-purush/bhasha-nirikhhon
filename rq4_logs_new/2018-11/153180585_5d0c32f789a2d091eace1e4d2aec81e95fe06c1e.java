package infrastructure.database;

import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import play.db.Database;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

public class JdbiFactory {

    private Database database;
    private Jdbi jdbi;

    public JdbiFactory() {
    }

    @Inject
    public JdbiFactory(Database database) {
        this.database = database;
        this.jdbi = Jdbi.create(this.database.getDataSource()).installPlugin(new SqlObjectPlugin());
    }

    public Jdbi getJdbi() {
        return this.jdbi;
    }
}