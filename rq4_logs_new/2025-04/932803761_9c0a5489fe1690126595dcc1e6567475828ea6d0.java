package uk.ac.bangor.cs.cambria.AcademiGymraeg.controller;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.Result;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.Test;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.User;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.TestRepository;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.UserRepository;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.util.ResultsService;

/**
 * @author jcj23xfb
 */

@Controller
@RequestMapping("/viewResults")
public class ViewResultsController {

	@Autowired
	private TestRepository testRepo;

	@Autowired
	private UserRepository userRepo;

	@Autowired
	private ResultsService rs;

	public static String editConfirmationMessage = "";

	@GetMapping
	public String viewUsers(Model m) {
		List<User> users = userRepo.findAll();

		if (users.isEmpty()) {
			return "viewresults";
		}

		User u = users.get(0);

		Test t1 = new Test(u, ZonedDateTime.now(), 20);
		t1.setEndDateTime(t1.getStartDateTime().plusMinutes(5));

		t1.setNumberCorrect(7);

		testRepo.save(t1);

		List<Test> tests = testRepo.findAllByUser(u);
		List<Result> results = rs.convertTestsToResults(tests);

		m.addAttribute("allresults", results);

		return "viewresults";
	}

}