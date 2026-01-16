import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by Darkrengarius on 06.06.2016.
 */
public class BoruvkaTest
{
    @Test
    public void testBoruvka()
    {
        String graphFile = "src\\boruvka.txt";
        ArrayList<ArrayList<Integer>> testMST = new ArrayList<>();
        int testSize = 7;
        for (int i = 0; i < testSize; i++) {
            testMST.add(new ArrayList<>());
        }

        testMST.get(0).add(1);
        testMST.get(0).add(3);
        testMST.get(1).add(0);
        testMST.get(1).add(4);
        testMST.get(2).add(4);
        testMST.get(3).add(0);
        testMST.get(3).add(5);
        testMST.get(4).add(1);
        testMST.get(4).add(2);
        testMST.get(4).add(6);
        testMST.get(5).add(3);
        testMST.get(6).add(4);

        Graph g = new Graph(graphFile);
        Boruvka b = new Boruvka(g.size_graph, g.graph);
        b.Boruvka();
        ArrayList<ArrayList<Integer>> mst = b.getMST();

        Assert.assertEquals(testMST, mst);
    }
}