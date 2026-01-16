package pl.krzysztofskul.organization.hospital.medicalFunctionTag;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/medical-function-tags")
public class MedicalFunctionTagController {

    private final MedicalFunctionTagService medicalFunctionTagService;

    @Autowired
    public MedicalFunctionTagController(MedicalFunctionTagService medicalFunctionTagService) {
        this.medicalFunctionTagService = medicalFunctionTagService;
    }

    @GetMapping("/all")
    public String getAllMedicalFunctionTags(Model model) {
        model.addAttribute("medicalFunctionTags", medicalFunctionTagService.loadAll());
        return "hospitals/medical-function-tags/all";
    }

    @GetMapping("/init")
    public String init() {
        if (medicalFunctionTagService.loadAll() == null || medicalFunctionTagService.loadAll().size() == 0) {
            medicalFunctionTagService.init();
        }
        return "redirect:/medical-function-tags/all";
    }

    @GetMapping("/delete/{id}")
    public String delete(
            @PathVariable Long id
    ) {
        medicalFunctionTagService.delete(id);
        return "redirect:/medical-function-tags/all";
    }

}