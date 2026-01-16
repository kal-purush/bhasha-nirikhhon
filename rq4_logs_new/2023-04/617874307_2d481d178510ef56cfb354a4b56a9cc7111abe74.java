
package acme.features.student.enrolment;

import acme.entities.enrolment.Enrolment;
import acme.framework.controllers.AbstractController;
import acme.roles.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;

@Controller
public class StudentEnrolmentController extends AbstractController<Student, Enrolment> {

	@Autowired
	protected StudentEnrolmentShowService showService;

	@Autowired
	protected StudentEnrolmentListService listService;

	@Autowired
	protected StudentEnrolmentCreateService createService;

	@Autowired
	protected StudentEnrolmentUpdateService updateService;

	@Autowired
	protected StudentEnrolmentFinaliseService finaliseService;


	@PostConstruct
	protected void initialise() {
		super.addBasicCommand("list", this.listService);
		super.addBasicCommand("show", this.showService);
		super.addBasicCommand("create", this.createService);
		super.addBasicCommand("update", this.updateService);
		super.addCustomCommand("finalise", "update", this.finaliseService);
	}

}