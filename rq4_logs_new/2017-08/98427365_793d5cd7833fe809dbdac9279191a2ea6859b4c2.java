package ua.kyiv.univerpulse.studentv2.mvc.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import ua.kyiv.univerpulse.studentv2.mvc.dto.FacultyDto;
import ua.kyiv.univerpulse.studentv2.mvc.service.RegistrationService;
import javax.servlet.http.HttpServletRequest;

@Controller
public class AdminController {

    private static final Logger logger = Logger.getLogger(AdminController.class);

    private RegistrationService registrationService;
    @Autowired
    public AdminController(RegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    @RequestMapping(value = "/admin", method = RequestMethod.GET)
    public String getAdminPage() {
        return "admin";
    }

    @RequestMapping(value = "/admin/faculties", method = RequestMethod.GET)
    public String getFacultiesPage(Model model) {
        model.addAttribute("faculties", registrationService.getAllFaculty());
        model.addAttribute("faculty", new FacultyDto());
        return "faculties";
    }

    @RequestMapping(value = "/admin/faculties", method = RequestMethod.POST)
    public String updateFacultiesPage(@ModelAttribute("faculty") FacultyDto facultyDto, Model model) {
        if(logger.isDebugEnabled())
            logger.debug("Update entity Faculty in AdminController: " + facultyDto);
        registrationService.updateFaculty(facultyDto);
        model.addAttribute("faculties", registrationService.getAllFaculty());
        return "faculties";
    }

    @RequestMapping(value = "/admin/faculties/insert", method = RequestMethod.POST)
    public String updateFacultiesPage(Model model, HttpServletRequest request) {
        FacultyDto dto = new FacultyDto();
        dto.setName(request.getParameter("u_name"));
        dto.setEvaluationDate(request.getParameter("u_evaluationDate"));
        dto.setPassingScore(Integer.valueOf(request.getParameter("u_passingScore").trim()));
        dto.setNumberOfStudents(Integer.valueOf(request.getParameter("u_numberOfStudents")));
        if(logger.isDebugEnabled())
            logger.debug("Insert entity Faculty in AdminController: " + dto);
        registrationService.saveFaculty(dto);
        model.addAttribute("faculties", registrationService.getAllFaculty());
        model.addAttribute("faculty", new FacultyDto());
        return "faculties";
    }
}