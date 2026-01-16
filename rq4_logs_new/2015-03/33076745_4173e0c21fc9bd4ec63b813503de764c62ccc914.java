package ca.droppings.swirl;

/**
 * http://www.mkyong.com/maven/how-to-create-a-jar-file-with-maven/
 *
 */


import org.junit.runner.JUnitCore;

import ca.droppings.swirl.utils.SwirlListener;

public class App {

	
    
   public static void main(String[] args) {
	   JUnitCore runner = new JUnitCore();
	   runner.addListener(new SwirlListener());
	   runner.run(ca.droppings.swirl.suites.SimpleSmokeSuite.class,ca.droppings.swirl.suites.SimplerSmokeSuite.class);
    }
}