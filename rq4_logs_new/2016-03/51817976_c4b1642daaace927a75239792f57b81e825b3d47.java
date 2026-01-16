/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KmeansTests;

import Kmeans.Centroid;
import Kmeans.Clustering;
import Kmeans.Text;
import java.util.ArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads
 */
public class ClusteringTest {
    
    public ClusteringTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

     @Test
     public void clustering() {
         Double[] space1 = new Double[10];
        space1[0] = 0.1;
        space1[1] = 0.2;
        space1[2] = 0.2;
        space1[3] = 0.0;
        space1[4] = 0.0;
        space1[5] = 0.0;
        space1[6] = 0.0;
        space1[7] = 0.0;
        space1[8] = 0.0;
        space1[9] = 0.0;
        Double[] space2 = new Double[10];
        space2[0] = 0.0;
        space2[1] = 0.2;
        space2[2] = 0.2;
        space2[3] = 0.1;
        space2[4] = 0.0;
        space2[5] = 0.0;
        space2[6] = 0.0;
        space2[7] = 0.0;
        space2[8] = 0.0;
        space2[9] = 0.0;
        Double[] space3 = new Double[10];
        space3[0] = 0.0;
        space3[1] = 0.0;
        space3[2] = 0.0;
        space3[3] = 0.0;
        space3[4] = 0.2;
        space3[5] = 0.2;
        space3[6] = 0.1;
        space3[7] = 0.0;
        space3[8] = 0.0;
        space3[9] = 0.0;
        Double[] space4 = new Double[10];
        space4[0] = 0.0;
        space4[1] = 0.0;
        space4[2] = 0.0;
        space4[3] = 0.0;
        space4[4] = 0.2;
        space4[5] = 0.2;
        space4[6] = 0.0;
        space4[7] = 0.1;
        space4[8] = 0.1;
        space4[9] = 0.1;
        //Create test texts
        Text text1 = new Text("Mads er dejlig", space1);
        Text text2 = new Text("Is er dejlig", space2);
        Text text3 = new Text("En sort kat", space3);
        Text text4 = new Text("En sort gryde med låg", space4);
        Clustering clusterClass = new Clustering();
        ArrayList<Text> texts = new ArrayList<>();
        //Add texts to corpus
        texts.add(text4); texts.add(text2); texts.add(text3); texts.add(text1);
        ArrayList<Centroid> result = clusterClass.prepareCluster(2, texts);
        
        for(Centroid c : result) {
            System.out.print("Cluster \n");
            for(Text t : c.GroupedDocument) {
                System.out.println(t.getContent() + "\n");
            }
        }
     }
}