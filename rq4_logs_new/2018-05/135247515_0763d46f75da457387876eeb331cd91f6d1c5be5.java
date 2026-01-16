package com.example.demo.controller;

import com.example.demo.domain.Version;
import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/version")
public class VersionController {

    @RequestMapping(method = RequestMethod.GET)
    public String getVersion() {
        return Version.getVerStr();
    }

    @RequestMapping(path = "/minor", method = RequestMethod.POST)
    public String updateMajor() {
        Version.updateMinor();
        return Version.getVerStr();
    }
}