package br.lassal.dbvcs.tatubola.text;

import br.lassal.dbvcs.tatubola.relationaldb.repository.OracleRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import com.github.vertical_blank.sqlformatter.SqlFormatter;
import com.github.vertical_blank.sqlformatter.languages.AbstractFormatter;

public class SqlNormalizer {

    // see: https://github.com/vertical-blank/sql-formatter/blob/master/src/main/java/com/github/vertical_blank/sqlformatter/SqlFormatter.java
    private static final SqlNormalizer defaultSql = new SqlNormalizer(SqlFormatter.of("sql"));
    private static final SqlNormalizer oracle = new SqlNormalizer(SqlFormatter.of("pl/sql"));


    public static SqlNormalizer getInstance(RelationalDBRepository repository){

        if(repository instanceof OracleRepository){
            return oracle;
        }
        else {
            return defaultSql;
        }
    }

    private AbstractFormatter sqlFormatter;

    private SqlNormalizer(AbstractFormatter sqlFormatter){
        this.sqlFormatter = sqlFormatter;
    }

    public String formatSql(String sql){
        return this.sqlFormatter.format(sql);
    }
}