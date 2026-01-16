package studentenrollment.Academicinfo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import studentenrollment.Academicinfo.model.AcademicInfo;
import studentenrollment.Academicinfo.service.AcademicInfoService;

import java.util.List;

@RestController
@RequestMapping("api/v1/academicInfo")
public class AcademicInfoController {

    private final AcademicInfoService academicInfoService;

    @Autowired
    public AcademicInfoController(AcademicInfoService academicInfoService) {
        this.academicInfoService = academicInfoService;
    }

    @PostMapping("/add")
    public AcademicInfo addNew(@RequestBody AcademicInfo academicInfo) {
        return this.academicInfoService.addNew(academicInfo);
    }

    @GetMapping("/all")
    public List<AcademicInfo> getAllDetails() {
        return this.academicInfoService.getAllDetails();
    }
}