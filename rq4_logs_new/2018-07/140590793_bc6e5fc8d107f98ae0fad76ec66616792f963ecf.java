package modirum.seleniumtest.features.steps.serenity;

import modirum.seleniumtest.pages.HomePage;
import modirum.seleniumtest.pages.Navigate;
import net.thucydides.core.annotations.Step;

public class NavigateSteps {
	
	  Navigate navigate; 
	  
	  @Step
	  public void open_new_window(String windowURL) {
			// TODO Auto-generated method stub
		  navigate.open_new_window(windowURL);
	  }
}