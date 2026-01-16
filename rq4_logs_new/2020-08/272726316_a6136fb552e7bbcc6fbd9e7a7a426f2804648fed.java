package com.gslab.talent.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.gslab.talent.constant.Constant;
import com.gslab.talent.model.Candidate;
import com.gslab.talent.model.Level;
import com.gslab.talent.model.User;
import com.gslab.talent.model.Vacancy;
import com.gslab.talent.service.LevelService;
import com.gslab.talent.service.UserService;

@RestController
@CrossOrigin(allowedHeaders = "*")
@RequestMapping(value = "/api")
public class LevelController {

	@Autowired
	LevelService levelServiceObj;
	
	@PostMapping(value = "/addLevel")
	public ResponseEntity<Void> createLevel(@RequestBody Level level) {
		levelServiceObj.createLevel(level);
		return new ResponseEntity<Void>(HttpStatus.OK);
	}
	
	@GetMapping(value = "/getListOfLevels",headers = Constant.ACCEPT_JSON, produces = MediaType.APPLICATION_JSON_VALUE)
	public List<Level> getAllLevels() {
		return levelServiceObj.getAllLevel();
	}
	
	@GetMapping(value = "/getLevelById/{levelId}")
	public Level getlevelById(@PathVariable int levelId) {
		return levelServiceObj.getUserById(levelId);
	}
	
	@DeleteMapping(value = "/deleteByLevel/{level}")
	public ResponseEntity<Void> deleteLevel(@PathVariable String level)
	{
		Level l = levelServiceObj.findByLevel(level);
		System.out.println("level is "+l);
		Integer i = l.getId();
		levelServiceObj.deleteLevelById(i);
		return new ResponseEntity<Void>(HttpStatus.OK);
	}
	
	/*@GetMapping(value = "getLevelByVacancyId")
	public List<Level> getLevelByVacancyId(@PathVariable int id)
	{
		return levelServiceObj.getLevelsByVacancyId(id);
	}
	*/
	
	/*@GetMapping(value = Constant.GET_LEVEL_BY_ID, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Level> getLevelById(@PathVariable int id) {
		Level levelObj = levelServiceObj.findById(id);
		if (levelObj == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<>(levelObj, HttpStatus.OK);
	}
	
	
	@PutMapping(value = "/addLevel/{id}", headers = Constant.ACCEPT_JSON)
	public ResponseEntity<Void> createLevel(@PathVariable int id,@RequestBody Vacancy levelObj, UriComponentsBuilder ucBuilder) {
		levelServiceObj.createLevelList(levelObj,id);
		HttpHeaders headers = new HttpHeaders();
		headers.setLocation(ucBuilder.path(Constant.GET_LEVEL_BY_ID).buildAndExpand(levelObj.getVacancyId()).toUri());
		return new ResponseEntity<Void>(headers, HttpStatus.CREATED);
	}*/
}