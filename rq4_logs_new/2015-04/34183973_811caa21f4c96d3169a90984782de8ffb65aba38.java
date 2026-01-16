/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storeconcept;

/**
 *
 * @author Max
 */
public class StoreCONCEPT {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
               java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new StoreCheckout().setVisible(true);
            }
           
        });
        //double outcome = StoreItems.Item1 + StoreItems.Item2;
        //System.out.println(outcome);
    }
    
}