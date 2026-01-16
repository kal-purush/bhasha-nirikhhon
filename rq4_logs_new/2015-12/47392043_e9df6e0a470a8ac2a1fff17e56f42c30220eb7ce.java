
package registrationclassadvice;

/**
 *
 * @author Daniel Nguyen
 */
public class Advising {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
       java.awt.EventQueue.invokeLater(new Runnable(){
           public void run() {
               new ViewAdvising().setVisible(true);
           }
            });
         
    }
      
}