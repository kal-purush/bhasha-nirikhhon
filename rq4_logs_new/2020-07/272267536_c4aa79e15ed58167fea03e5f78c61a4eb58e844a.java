package br.lassal.dbvcs.tatubola.fs;

import br.lassal.dbvcs.tatubola.relationaldb.model.Index;
import br.lassal.dbvcs.tatubola.relationaldb.model.Table;
import br.lassal.dbvcs.tatubola.relationaldb.model.TableColumn;
import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import br.lassal.dbvcs.tatubola.text.TextDeserializer;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class BaseDBModelFSTest {

    private final String rootPath = "root";

    @Test
    public void testSaveIndex() throws IOException, DBModelFSException {
        JacksonYamlSerializer serializer = new JacksonYamlSerializer();
        InMemoryTestDBModelFS dbModelFS = new InMemoryTestDBModelFS(this.rootPath, serializer);

        String schema = "AAA";
        String indexName = "PK_Dummy";
        String tableName = "TAB_Dummy";

        Index index = new Index(schema, indexName, schema, tableName, "PRIMARY KEY", true);

        Path outputPath = dbModelFS.save(index);

        Path expectedOutputPath = Paths.get("root/AAA/Tables/Indexes/TABLE_" + tableName + "_INDEX_" +
                indexName + ".yaml");

        assertEquals(expectedOutputPath, outputPath);

        Index deserializedIndex = serializer.fromYAMLtoPOJO(dbModelFS.getSerializationInfo(index).getYamlText(), Index.class);
        assertEquals(index, deserializedIndex);
    }

    @Test
    public void testSaveTrigger() throws IOException, DBModelFSException {
        JacksonYamlSerializer serializer = new JacksonYamlSerializer();
        InMemoryTestDBModelFS dbModelFS = new InMemoryTestDBModelFS(this.rootPath, serializer);

        String triggerName = "Log_Change";
        Trigger trigger = new Trigger(triggerName);
        trigger.setTargetObjectSchema("BBB");
        trigger.setTargetObjectName("TAB_Dummy");
        trigger.setTargetObjectType("TABLE");
        trigger.setEvent("INSERT ROW");
        trigger.setEventTiming("AFTER");
        trigger.setEventActionBody(" set last_change=sysdate() ");

        Path outputPath = dbModelFS.save(trigger);

        Path expectedOutputPath = Paths.get("root/BBB/Tables/Triggers/TABLE_TAB_Dummy_TRIGGER_Log_Change.yaml");

        assertEquals(expectedOutputPath, outputPath);

        Trigger deserializedTrigger = serializer.fromYAMLtoPOJO(dbModelFS.getSerializationInfo(trigger).getYamlText(), Trigger.class);

        assertEquals(trigger, deserializedTrigger);
    }

    /**
     * Create a sample Table (partially filled) and verify if the output has this format
     *
     *  /{SCHEMA}/Tables/TABLE_{table name}.yaml
     *
     *  Serialize this table in textual format and checks if the deserialized version is identical
     *
     */
    @Test
    public void testSaveTable() throws IOException, DBModelFSException {
        JacksonYamlSerializer serializer = new JacksonYamlSerializer();
        InMemoryTestDBModelFS dbModelFS = new InMemoryTestDBModelFS(this.rootPath, serializer);

        Table table = new Table("CCC", "TBEmployee", true);
        table.addColumn(this.createNumericTableColumn("IDEmployee", 20, false, 1));
        table.addColumn(this.createTextTableColumn("FirstName", 20, false, 2));
        table.addColumn(this.createTextTableColumn("LastName", 40, false, 3));

        Path outputPath = dbModelFS.save(table);
        Path expectedOutputPath = Paths.get(this.rootPath + "/CCC/Tables/TABLE_TBEmployee.yaml");
        assertEquals(expectedOutputPath, outputPath);

        Table deserializedTable = serializer.fromYAMLtoPOJO(dbModelFS.getSerializationInfo(table).getYamlText(), Table.class);
        assertEquals(table, deserializedTable);


    }

    private TableColumn createNumericTableColumn(String columnName,int precision, boolean isNullable, int position){
        TableColumn column = new TableColumn(columnName);
        column.setDataType("NUMBER");
        column.setNumericPrecision(precision);
        column.setNullable(isNullable);
        column.setOrdinalPosition(position);

        return column;
    }

    private TableColumn createTextTableColumn(String columnName,long columnLength, boolean isNullable, int position){
        TableColumn column = new TableColumn(columnName);
        column.setDataType("VARCHAR");
        column.setTextMaxLength(columnLength);
        column.setNullable(isNullable);
        column.setOrdinalPosition(position);

        return column;
    }
}