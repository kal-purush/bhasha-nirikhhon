package com.codeup.mentor.controllers;


        import org.springframework.stereotype.Controller;
        import org.springframework.web.bind.annotation.GetMapping;
        import org.springframework.web.bind.annotation.ResponseBody;


@Controller
public class HelloWorldController {

//    @GetMapping("/hello")
//    public String sayHello(){
//    return "helloworld.html";
//    }
//

    @GetMapping("/hello")
    @ResponseBody
    public String sayHelloRB(){
        return "Hello from the downtowners!";
    }
}