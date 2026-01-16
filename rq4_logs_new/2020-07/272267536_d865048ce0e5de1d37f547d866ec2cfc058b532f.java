package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.InMemoryTestDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TriggerSerializerTest extends BaseSerializerTest{


    /**
     * Test how the serialization process works for Triggers
     * Checks how the full process works:
     *   - assemble (a single method here)
     *   - serialization
     *   - tidy up properties : in this case manual verification
     * @throws Exception
     */
    @Test
    public void testTriggerSerializer() throws Exception {
        String env = "DEV";
        String schema = "ABC";

        InMemoryTestDBModelFS dbModelFS = this.createNewDBModelFS();
        TriggerSerializer serializer = new TriggerSerializer(this.repository, dbModelFS, schema, env);

        List<Trigger> sourceTriggers = new ArrayList<>();
        sourceTriggers.add(this.createSampleTriggerA(5, schema, "TB_FIRST"));
        sourceTriggers.add(this.createSampleTriggerC(1, schema, "TB_FIFTH"));
        sourceTriggers.add(this.createSampleTriggerB(3, schema, "TB_THIRD"));
        sourceTriggers.add(this.createSampleTriggerB(4, schema, "TB_FOURTH"));
        sourceTriggers.add(this.createSampleTriggerA(2, schema, "TB_SECOND"));

        when(this.repository.loadTriggers(schema)).thenReturn(sourceTriggers);

        serializer.serialize();

        for(Trigger sourceTrigger: sourceTriggers){
            InMemoryTestDBModelFS.SerializationInfo serializationInfo = dbModelFS.getSerializationInfo(sourceTrigger);

            assertEquals(sourceTrigger, serializationInfo.getDBObject());
        }

        verify(this.repository, times(1)).loadTriggers(schema);
        assertTrue(dbModelFS.getNumberSerializedObjects() > 2);
    }

    /**
     * Test if the deserialized version of a Trigger in textual format is identical to the
     * original Trigger
     * @throws Exception
     */
    @Test
    public void testSerializeDeserializeTrigger() throws Exception {
        String env = "DEV";
        String schema = "ABC";

        InMemoryTestDBModelFS dbModelFS = this.createNewDBModelFS();
        TriggerSerializer serializer = new TriggerSerializer(this.repository, dbModelFS, schema, env);

        List<Trigger> sourceTriggers = new ArrayList<>();
        sourceTriggers.add(this.createSampleTriggerA(1, schema, "TB_FIRST"));

        when(this.repository.loadTriggers(schema)).thenReturn(sourceTriggers);
        serializer.serialize();

        Trigger sourceTrigger = sourceTriggers.get(0);
        InMemoryTestDBModelFS.SerializationInfo serializationInfo = dbModelFS.getSerializationInfo(sourceTrigger);

        assertEquals(sourceTrigger, serializationInfo.getDBObject());

        Trigger deserializedTrigger = this.textSerializer.fromYAMLtoPOJO(serializationInfo.getYamlText(), Trigger.class);

        assertEquals(sourceTrigger, deserializedTrigger);

    }

    /**
     * Create sample trigger for the test.
     * The trigger body is not formatted, it will be formatted during the test
     * @param id
     * @param schema
     * @param targetObjectName
     * @return
     */
    private Trigger createSampleTriggerA(int id, String schema, String targetObjectName){
        Trigger trigger = new Trigger("TRIGGER_A_" + id);
        trigger.setTargetObjectType("TABLE");
        trigger.setTargetObjectSchema(schema);
        trigger.setTargetObjectName(targetObjectName);
        trigger.setExecutionOrder(id);
        trigger.setEvent("UPDATE");
        trigger.setEventTiming("AFTER EACH ROW");
        trigger.setEventActionBody("  BEGIN add_job_history(:old.employee_id,:old.hire_date,sysdate,:old.job_id," +
               ":old.department_id);END;");

        return trigger;
    }

    private Trigger createSampleTriggerB(int id, String schema, String targetObjectName){
        Trigger trigger = new Trigger("TRIGGER_B_" + id);
        trigger.setTargetObjectType("TABLE");
        trigger.setTargetObjectSchema(schema);
        trigger.setTargetObjectName(targetObjectName);
        trigger.setExecutionOrder(id);
        trigger.setEvent("INSERT OR UPDATE OR DELETE");
        trigger.setEventTiming("BEFORE STATEMENT");
        trigger.setEventActionBody("  BEGIN secure_dml; END secure_employees;");

        return trigger;
    }

    private Trigger createSampleTriggerC(int id, String schema, String targetObjectName){
        Trigger trigger = new Trigger("TRIGGER_C_" + id);
        trigger.setTargetObjectType("TABLE");
        trigger.setTargetObjectSchema(schema);
        trigger.setTargetObjectName(targetObjectName);
        trigger.setExecutionOrder(id);
        trigger.setEvent("INSERT on ROW");
        trigger.setEventTiming("BEFORE");
        trigger.setEventActionBody("BEGIN SET NEW.entryDate = NOW(); END");

        return trigger;
    }
}