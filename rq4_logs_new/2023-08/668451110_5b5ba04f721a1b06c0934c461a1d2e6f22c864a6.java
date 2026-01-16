package com.example.successPie;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

/** Controller class for handling HTTP requests related to pie chart. */
@Controller
@RequestMapping(path = "/pie")
public class PieController {

  private final PieRepository pieRepository;

  /**
   * Constructor to initialize the PieController with a PieRepository.
   *
   * @param pieRepository The repository for accessing pie data.
   */
  @Autowired
  public PieController(PieRepository pieRepository) {
    this.pieRepository = pieRepository;
  }

  /**
   * Handles GET requests to display the pie chart. Retrieves pie data from the repository and adds
   * it to the model.
   *
   * @param model The model to be populated with data for the view.
   * @return The name of the Thymeleaf template for rendering the pie chart.
   */
  @GetMapping(value = {"", "/"})
  public String getPies(Model model) {
    // Get user-specific success percentages
    int myPercentValue = pieRepository.getMyPercentValue("basket0810");
    int leftPercentValue = pieRepository.getLeftPercentValue("basket0810");

    // Add percentages to the model for view rendering
    model.addAttribute("myPercentValue", myPercentValue);
    model.addAttribute("leftPercentValue", leftPercentValue);

    // Return the name of the Thymeleaf template
    return "pie/index";
  }
}