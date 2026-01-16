package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Test for circular references
 */
public class TestCircularRefs extends BaseAvroTest {

  @Test
  public void testCircularRefs() throws Exception {

    log("\n\n----- Creating entities -----");
    CircularRefList head = new CircularRefList();
    head.setA(10);
    CircularRefList tail = new CircularRefList();
    tail.setA(20);

    head.setNext(tail);
    tail.setNext(head);

    log("\n\n----- Testing serialization -----");
    byte[] bytes = serialize(head);

    log("\n\n----- Testing generic datum read -----");
    List<GenericRecord> genRecords =
      (List<GenericRecord>) genericDatumRead(bytes);
    GenericRecord genRecord = genRecords.get(0);
    assertNotNull ("Cannot read element 'next'", genRecord.get("next"));

    log("\n\n----- Testing reflect datum read -----");
    List<CircularRefList> appRecords =
      (List<CircularRefList>) reflectDatumRead(bytes, CircularRefList.class);
    CircularRefList appRecord = appRecords.get(0);
    log ("Read: " + appRecord);
    assertNotNull ("Unable to read app-record", appRecord);
    assertNotNull ("Unable to read app-record's child element", appRecord.getNext());
    assertNotNull ("Unable to read circular ref", appRecord.getNext().getNext());
    assertTrue ("Circular ref not formed properly", appRecord.getNext().getNext() instanceof CircularRefList);

    log("\n\n----- Testing JSON encoder/decoder -----");
    byte[] jsonBytes = encodeToJson (head);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = decodeJson(jsonBytes, head);
    assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
  }
}

class CircularRefList {

    Integer a;
    CircularRefList next;

    public Integer getA() {
        return a;
    }
    public void setA(Integer a) {
        this.a = a;
    }
    public CircularRefList getNext() {
        return next;
    }
    public void setNext(CircularRefList next) {
        this.next = next;
    }
}
