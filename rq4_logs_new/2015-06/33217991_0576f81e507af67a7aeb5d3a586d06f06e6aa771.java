package edu.mc2.sms.cntrlr.rest;


import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import edu.mc2.sms.jpa.entity.Course;
import edu.mc2.sms.service.CourseService;

@RestController
@RequestMapping("/course")
public class CourseRestController {

	@Autowired
	private CourseService courseService;
	

	@RequestMapping(method = RequestMethod.GET)
	public List<Course> getCourses(@RequestParam(value = "name" , required = false) String name) {
		if(name == null || name.isEmpty()){
			return courseService.getCourses();
		}else{
			return courseService.getCourses(name);
		}
	}
	
	
	@RequestMapping(value = "/{id}", method = RequestMethod.GET)
	public Course getCourse(@PathVariable int id) {
		return courseService.getCourse(id);
	}

	
	
	@RequestMapping(method = RequestMethod.POST)
	public void createCourse(@RequestBody Course course, UriComponentsBuilder uriBuilder,HttpServletResponse response) {
		courseService.createCourse(course);
		response.setStatus(HttpStatus.SC_CREATED);
		response.setHeader("Location", uriBuilder.path("/course/{id}")
				.buildAndExpand(course.getId()).toUriString());
			
	}
	
	
	
	@RequestMapping(value = "/{id}",method = RequestMethod.PUT)
	public void editStudent(@PathVariable int id,@RequestBody Course course, HttpServletResponse response) {
		courseService.editCourse(id, course);
	    response.setStatus(HttpStatus.SC_ACCEPTED);
	}
	
	
	
	@RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
	public void deleteStudent(@PathVariable int id,HttpServletResponse response) {
		courseService.deleteCourse(id);
		response.setStatus(HttpStatus.SC_OK);
	}
	
	
}