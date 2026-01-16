import hu.bme.mit.train.Tachograph;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class test2 {

    @Test
    public void TestTachograph(){
        Tachograph tachograph = new Tachograph();
        tachograph.addToTable("elem","elem", "elem");
        assertFalse(tachograph.tab.isEmpty());
    }
}