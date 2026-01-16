package io.egen.apollo.controller;

import java.util.List;

import io.egen.apollo.entity.Movie;
import io.egen.apollo.entity.MovieType;
import io.egen.apollo.exceptions.MovieAlreadyExistsException;
import io.egen.apollo.exceptions.MovieNotFoundException;
import io.egen.apollo.service.MovieService;
import io.egen.apollo.service.Movie_TypeService;
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
@RequestMapping("/movie_type")
@Api(tags="movie_type")
public class Movie_Type_Controller {
	
	@Autowired
	private Movie_TypeService service;
	
	@RequestMapping(method=RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Find All Movie Type",
	notes="Returns a list of the movie type in the system.")	
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public List<MovieType> findAll()
	{		
		return service.findAllMovieType();		
	}
	
	@RequestMapping(value="{id}", method=RequestMethod.GET, produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Find Movie Type By Id",
	notes="Returns a movie type by it's if it exists.")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=404, message="Not Found"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public MovieType findOne(@PathVariable("id") String id) {
		return service.findMovieTypeById(id);
	}

	@RequestMapping(method=RequestMethod.POST, 
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Create Movie Type",
	notes="Create and return movie type")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=400, message="Bad Request"),
		@ApiResponse(code=500, message="Internal Server Error")
	})	
	public MovieType create(@RequestBody MovieType movieType){
		return service.createMovieType(movieType);
	}
	
	@RequestMapping(value="{id}", 
			method=RequestMethod.PUT,
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Update Movie Type",
	notes="Update an existing movie type")
	@ApiResponses(value={
		@ApiResponse(code=200, message="Success"),
		@ApiResponse(code=400, message="Bad Request"),
		@ApiResponse(code=404, message="Not Found"),
		@ApiResponse(code=500, message="Internal Server Error")
	})
	public MovieType update (@PathVariable("id") String id, @RequestBody MovieType MovieType) throws MovieNotFoundException {
		return service.updateMovieType(id, MovieType);
	}
	
	@RequestMapping(value="{id}", 
			method=RequestMethod.DELETE,
			consumes=MediaType.APPLICATION_JSON_VALUE,
			produces=MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value="Deleted Movie Type",
	notes="Update an existing movie type")
	public void delete(@PathVariable("id") String id) {		
		MovieType mv = findOne(id);
		service.deleteMovieType(mv);
	}	

}