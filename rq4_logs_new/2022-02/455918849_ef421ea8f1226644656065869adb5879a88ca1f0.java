package com.wipro.usermgmt.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.wipro.usermgmt.entities.User;
import com.wipro.usermgmt.service.UserManagementService;

/**
 * @author Venkat
 * 
 * Description : used to handle user management requests and response
 *
 */
@RestController
public class UserManagementController {

	private static final Logger Log = LoggerFactory.getLogger(UserManagementController.class);

	private UserManagementService usrMgmtService;

	@Autowired
	public void setUsrMgmtService(UserManagementService usrMgmtService) {
		this.usrMgmtService = usrMgmtService;
	}

	/**
	 * Description: This API used to create User
	 * 
	 * @param user
	 * @return
	 */
	@PostMapping("/createuser")
	public ResponseEntity<User> createUser(@RequestBody User user) {
		Log.info("createuser API invoked for username : {} ", user.getUserName());
		User createdUser = usrMgmtService.createUser(user);
		Log.info("end of createuser API for username : {} ", user.getUserName());
		return new ResponseEntity<User>(createdUser, HttpStatus.OK);
	}

	/**
	 * Description: This API used to update user
	 * 
	 * @param user
	 * @return
	 */
	@PutMapping("/updateuser")
	public ResponseEntity<User> updateUser(@RequestBody User user) {
		Log.info("updateuser API invoked for username : {} ", user.getUserName());
		User updatedUser = usrMgmtService.updateUser(user);
		Log.info("end of updateuser API for username : {} ", user.getUserName());
		return new ResponseEntity<User>(updatedUser, HttpStatus.OK);
	}

	/**
	 * Description: This API used to get all users
	 * 
	 * @return
	 */
	@GetMapping("/getusers")
	public ResponseEntity<List<User>> getUsers() {
		Log.info("getUsers API invoked.");
		List<User> allUsers = usrMgmtService.getAllUser();
		Log.info("end of getUsers API.");
		return new ResponseEntity<List<User>>(allUsers, HttpStatus.OK);
	}

	/**
	 * Description: This API used to update user
	 * 
	 * @param id
	 * @return
	 */
	@DeleteMapping("/deleteuser")
	public ResponseEntity<String> deleteUser(@RequestParam Long id) {
		Log.info("deleteUser API invoked for username : {} ", id);
		usrMgmtService.deleteUser(id);
		Log.info("end of deleteUser API for username : {} ", id);
		return new ResponseEntity<String>("Success", HttpStatus.OK);
	}

}