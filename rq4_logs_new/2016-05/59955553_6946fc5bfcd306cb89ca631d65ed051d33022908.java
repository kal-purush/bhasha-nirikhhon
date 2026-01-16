package net.bluetab.bigdataactivity.testcassandra;

import com.datastax.driver.core.Cluster;

public class Main {

   private Cluster cluster;

   public void connect(String node) {
      cluster = Cluster.builder()
            .addContactPoint(node).build();
   }

   public void close() {
      cluster.close();
   }

   public static void main(String[] args) {
      Main client = new Main();
      client.connect("reto3db");
      for (int i = 0; i < 1; i++) {
         System.out.println ("Hello World !!! \n");
      } 
      client.close();
   }
}