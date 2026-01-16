package uz.pdp.simline.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uz.pdp.simline.service.SimCardService;
import uz.pdp.simline.util.Generator;

@RestController
@RequestMapping("/test")
@RequiredArgsConstructor
public class Controller {
    private final SimCardService simCardService;
    private final Generator generator;
    @GetMapping("/generate")
    public String  generate(){
        simCardService.addAll(generator.generator());
        return "Success";
    }
}