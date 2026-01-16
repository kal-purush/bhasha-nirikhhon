
package acme.features.student.enrolment;

import acme.entities.enrolment.Enrolment;
import acme.entities.lecture.Course;
import acme.entities.tutorial.Tutorial;
import acme.entities.tutorialSession.TutorialSession;
import acme.features.assistant.tutorial.AssistantTutorialRepository;
import acme.framework.components.accounts.Principal;
import acme.framework.components.jsp.SelectChoices;
import acme.framework.components.models.Tuple;
import acme.framework.services.AbstractService;
import acme.roles.Assistant;
import acme.roles.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;

@Service
public class StudentEnrolmentDeleteService extends AbstractService<Student, Enrolment> {

	// Internal state ---------------------------------------------------------

	@Autowired
	protected StudentEnrolmentRepository repository;

	// AbstractService interface ----------------------------------------------


	@Override
	public void check() {
		boolean status;

		status = super.getRequest().hasData("id", int.class);

		super.getResponse().setChecked(status);
	}

	@Override
	public void authorise() {

		int id = super.getRequest().getData("id", int.class);
		Enrolment object = this.repository.findById(id);

		final Principal principal = super.getRequest().getPrincipal();
		final int userAccountId = principal.getAccountId();
		final boolean enrolmentIsNotFinalised = !object.isFinished();
		final boolean assistantOwnsTutorial = object.getStudent().getUserAccount().getId() == userAccountId;

		super.getResponse().setAuthorised(enrolmentIsNotFinalised && assistantOwnsTutorial);
	}

	@Override
	public void load() {
		int id = super.getRequest().getData("id", int.class);
		Enrolment object = this.repository.findById(id);

		super.getBuffer().setData(object);
	}

	@Override
	public void bind(final Enrolment object) {
		assert object != null;

		super.bind(object, "code", "motivation", "goals");
	}

	@Override
	public void validate(final Enrolment object) {
		assert object != null;

	}

	@Override
	public void perform(final Enrolment object) {
		assert object != null;

		this.repository.delete(object);
	}

	@Override
	public void unbind(final Enrolment object) {
		assert object != null;

		Tuple tuple = super.unbind(object, "code", "motivation", "goals");

		super.getResponse().setData(tuple);
	}

}