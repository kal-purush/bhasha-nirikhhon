package com.webvidhi.pubsub.controller;



import java.util.List;
import java.util.Map;

import javax.websocket.server.PathParam;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.webvidhi.pubsub.modal.APIKey;
import com.webvidhi.pubsub.modal.Device;
import com.webvidhi.pubsub.modal.User;
import com.webvidhi.pubsub.service.UserService;

import retrofit2.http.PUT;



@RestController
@CrossOrigin

@RequestMapping("/v1/users")
public class UserController {
	
	@Autowired
	UserService userService;
	
	@GetMapping("/")
	public User getUser(@RequestParam String userId) {
		return userService.getUser(userId);
		 
	}
	@PostMapping("/")
	public boolean createUser(@RequestBody User user) {
		userService.createOrUpdate(user);
		return true;
	}
	
	
	@PostMapping("/apikey")
	public String generateAPIKey() {
		
		 String generatedString = RandomStringUtils.randomAlphanumeric(30);
		
		return generatedString;
	}
	
	@PutMapping("/apiKey")
	public void saveKey(@RequestBody Map<String, String> keyInfo) { 
		
		userService.addKeys(keyInfo.get("userID"), keyInfo.get("keyID"), keyInfo.get("description"));
		
		
	}
	
	@GetMapping("/apiKey")
	public List<APIKey> saveKey(@RequestParam String userID) { 
		
		return userService.getAllKeys(userID);
		
		
	}
	@DeleteMapping("/apiKey")
	public void deleteKey(@RequestParam String userID,@RequestParam String keyID) {
		userService.removeKey(userID,keyID);
	}
	
	@PostMapping("/validate")
	public String validate(@RequestBody Map<String,String> userInfo) {
		return userService.validate(userInfo.get("user"), userInfo.get("pass"));
	}
	
	@GetMapping("/key/{key}")
	public User getUserBykey(@PathVariable String key) {
		return userService.getUserByKey(key);
		 
	}
	@GetMapping("/devices")
	public List<Device> getDevices(@RequestParam String userId) {
		return userService.getAllDevices(userId);
		 
	}
	
}