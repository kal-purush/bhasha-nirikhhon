package com.autohome.mail.controller;

import com.autohome.common.vo.R;
import com.autohome.mail.servcer.MailServcer;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping
@Api("api/mail")
public class MailController {
    @Autowired
    private MailServcer mailServcer;
    @GetMapping("/sendMail")
    public R sendMail(@RequestParam String to) {
        R r = mailServcer.sendMail(to);
        return r;

    }
}