package le.ac.uk.controller;

import le.ac.uk.entity.activity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/activity")
public class activityController {

    @Autowired
    private activity activity;

    @GetMapping("/")
    protected activity getActivity() {
        return activity;
    }
}