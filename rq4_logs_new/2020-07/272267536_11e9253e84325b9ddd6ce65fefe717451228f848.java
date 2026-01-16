package br.lassal.dbvcs.tatubola.integration.fs;

import br.lassal.dbvcs.tatubola.fs.BaseDBModelFS;
import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.fs.DBModelFSException;
import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseDBModelFSTest {

    private final String rootPath = "dsTests";

    @Before
    public void cleanUpOutputFiles() throws IOException {

        this.deleteDir(new File(this.rootPath));

    }

    private void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }

    @Test
    public void testSerializeTable() throws IOException, DBModelFSException {
        DBModelFS dfs = new BaseDBModelFS(this.rootPath, new JacksonYamlSerializer());
        Table table = this.buildTable();

        dfs.save(table);

        Path expectedPath = Paths.get(this.rootPath, table.getSchema().toUpperCase(),"Tables", "TABLE_" + table.getName() + ".yaml");
        File outputFile = expectedPath.toFile();

        assertTrue(outputFile.exists());

        Table serializedTable =  dfs.loadFromFS(expectedPath, table.getClass());

        assertEquals(table, serializedTable);

    }

    private Table buildTable(){
        Table table = new Table("schemaA", "TableA");
        TableColumn col1 = new TableColumn("Column1");
        col1.setOrdinalPosition(1);
        col1.setNullable(false);
        col1.setDataType("NUMBER");
        col1.setDefaultValue("27");
        col1.setNumericPrecision(8);
        col1.setNumericScale(3);
        table.addColumn(col1);

        TableColumn col2 = new TableColumn("Column2");
        col2.setOrdinalPosition(2);
        col2.setNullable(false);
        col2.setDataType("VARCHAR");
        col2.setDefaultValue("-- default value");
        col2.setTextMaxLength(50L);
        table.addColumn(col2);

        UniqueConstraint constr1 = new UniqueConstraint(table.getSchema(), table.getName(), "PK_01", ConstraintType.PRIMARY_KEY);
        constr1.addColumn(new Column("Column1", 1));
        constr1.addColumn(new Column("Column2", 2));

        table.addConstraint(constr1);

        CheckConstraint constr2 = new CheckConstraint(table.getSchema(), table.getName(), "Check_something", "Column1 > 0");
        table.addConstraint(constr2);

        return table;
    }
}