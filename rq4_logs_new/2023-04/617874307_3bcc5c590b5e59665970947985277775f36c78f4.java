
package acme.features.assistant.assistantDashboard;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import acme.forms.AssistantDashboard;
import acme.framework.controllers.AbstractController;
import acme.roles.Assistant;

@Controller
public class AssistantAssistantDashboardController extends AbstractController<Assistant, AssistantDashboard> {

	// Internal state ---------------------------------------------------------

	@Autowired
	protected AssistantAssistantDashboardShowService showService;

	// Constructors -----------------------------------------------------------


	@PostConstruct
	protected void initialise() {
		super.addBasicCommand("show", this.showService);
	}

}