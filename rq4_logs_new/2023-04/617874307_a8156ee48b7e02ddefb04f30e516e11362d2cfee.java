
package acme.features.lecturer.lecture;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import acme.entities.lecture.Lecture;
import acme.framework.controllers.AbstractController;
import acme.roles.Lecturer;

@Controller
public class LecturerLectureController extends AbstractController<Lecturer, Lecture> {

	@Autowired
	protected LecturerLectureListService	listService;

	@Autowired
	protected LecturerLectureShowService	showService;

	@Autowired
	protected LecturerLectureListAllService	listAllService;

	@Autowired
	protected LecturerLectureCreateService	createService;

	@Autowired
	protected LecturerLecturePublishService	publishService;

	@Autowired
	protected LecturerLectureUpdateService	updateService;

	@Autowired
	protected LecturerLectureDeleteService	deleteService;


	@PostConstruct
	protected void initialise() {
		super.addBasicCommand("list", this.listService);
		super.addBasicCommand("show", this.showService);
		super.addCustomCommand("list-all", "list", this.listAllService);
		super.addBasicCommand("create", this.createService);
		super.addCustomCommand("publish", "update", this.publishService);
		super.addBasicCommand("update", this.updateService);
		super.addBasicCommand("delete", this.deleteService);
	}
}