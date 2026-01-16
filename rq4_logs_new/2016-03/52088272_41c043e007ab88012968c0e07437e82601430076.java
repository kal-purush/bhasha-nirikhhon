package io.egen.apollo.controller;

import java.util.List;

import io.egen.apollo.entity.Movie;
import io.egen.apollo.entity.Comment;
import io.egen.apollo.exceptions.MovieAlreadyExistsException;
import io.egen.apollo.exceptions.MovieNotFoundException;
import io.egen.apollo.service.MovieService;
import io.egen.apollo.service.CommentService;
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
@RequestMapping("/comment")
@Api(tags="comment")
public class CommentController {
	
	@Autowired
	private CommentService service;
	
	@RequestMapping(method=RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Find All Movie Comments",
	notes="Returns a list of the movie comments in the system.")	
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public List<Comment> findAll()
	{		
		return service.findAllMovieComment();		
	}
	
	@RequestMapping(value="{id}", method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Find Movie Comments By Id",
	notes="Returns a movie comments by it's if it exists.")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=404, message="Not Found"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public Comment findOne(@PathVariable("id") String id) {
		return service.findMovieCommentById(id);
	}

	@RequestMapping(method=RequestMethod.POST, 
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Create Movie Comments",
	notes="Create and return movie comments")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=400, message="Bad Request"),
		@ApiResponse(code=500, message="Internal Server Error")
	})	
	public Comment create(@RequestBody Comment comment){
		return service.createMovieComment(comment);
	}
	
	@RequestMapping(value="{id}", 
			method=RequestMethod.PUT,
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Update Movie Comments",
	notes="Update an existing movie comments")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=400, message="Bad Request"),
		@ApiResponse(code=404, message="Not Found"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public Comment  update (@PathVariable("id") String id, @RequestBody Comment Comment) throws MovieNotFoundException {
		return service.updateMovieComment(id, Comment);
	}
	
	@RequestMapping(value="{id}", 
			method=RequestMethod.DELETE,
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Deleted Movie Comments",
	notes="Update an existing movie comments")
	public void delete(@PathVariable("id") String id) {		
		Comment mv = findOne(id);
		service.deleteMovieComment(mv);
	}	

}