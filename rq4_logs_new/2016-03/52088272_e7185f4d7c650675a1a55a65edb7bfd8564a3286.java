package io.egen.apollo.controller;

import java.util.List;

import io.egen.apollo.entity.User;
import io.egen.apollo.exceptions.UserAlreadyExistsException;
import io.egen.apollo.exceptions.UserNotFoundException;
import io.egen.apollo.service.UserService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/users")
@Api(tags="users")
public class UserController {
	
	@Autowired
	private UserService service;
	
	@RequestMapping(method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Find All Users",
			notes="Returns a list of the users in the system.")
	@ApiResponses(value={
			@ApiResponse(code=200, message="Success"),
			@ApiResponse(code=500, message="Internal Server Error")
	})
	public List<User> findAll () {
		return service.findAllUsers();
	}
	
	@RequestMapping(value="{id}", method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Find User By Id",
	notes="Returns a user by it's id if it exists.")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=404, message="Not Found"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public User findOne(@PathVariable("id") String id) throws UserNotFoundException {
		return service.findUserById(id);
	}
	
	@RequestMapping(method=RequestMethod.POST, 
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Create User",
	notes="Create and return user")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=400, message="Bad Request"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public User create (@RequestBody User user) throws UserAlreadyExistsException {
		return service.createUser(user);
	}
	
	@RequestMapping(value="{id}", 
			method=RequestMethod.PUT,
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Update User",
	notes="Update an existing user")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=400, message="Bad Request"),
		@ApiResponse(code=404, message="Not Found"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public User update (@PathVariable("id") String id, @RequestBody User user) throws UserNotFoundException {
		return service.updateUser(id, user);
	}
	
	@RequestMapping(value="{id}", method=RequestMethod.DELETE, produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Delete User",
	notes="Delete an existing user")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=404, message="Not Found"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public void delete (@PathVariable("id") String id) throws UserNotFoundException {
		service.deleteUser(id);
	}
}