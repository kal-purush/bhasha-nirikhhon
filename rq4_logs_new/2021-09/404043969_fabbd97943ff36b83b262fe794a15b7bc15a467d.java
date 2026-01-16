package fi.pellikka.hello.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class HelloController {

	@RequestMapping("hello")
	@ResponseBody
	public String returnGreetingForName(@RequestParam(name ="firstname", 
	required=false, defaultValue="Muumi") String name,
	@RequestParam(name="age") int ika) {
	return "Hello " + name + ", " + ika + " years old";
	}
}