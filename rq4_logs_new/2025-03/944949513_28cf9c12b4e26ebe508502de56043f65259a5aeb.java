package le.ac.uk.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

//@RestController
//@RequestMapping("/")
//public class activityController {
//
//    @Autowired
//    private activityDAO activity;
//
//    @GetMapping("/")
//    protected activityDAO getActivity() {
//        return activity;
//    }
//}

@Controller
public class ActivityController {
    @GetMapping("/home")
    protected String index() {
        return "index";
    }
}