package jp.co.namihira.townbookweb.controller.view.eventinfo;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class EventInfoController {
        
    @GetMapping("/eventinfo")
    public String get() {
        return "eventinfo/body";
    }    
    
}