package com.example.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
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

import com.example.model.Memo;
import com.example.model.MemoGroup;
import com.example.model.User;
import com.example.service.MemoGroupService;
import com.example.service.MemoService;
import com.example.service.UserService;

@RestController
@CrossOrigin(origins="*")
@RequestMapping(value="/memogroup")
public class MemoGroupController {
	
	MemoGroupService mgServe;
	UserService uServe;
	MemoService mServe;

	@Autowired
	public MemoGroupController(MemoGroupService mgServe, UserService userve, MemoService mServe) {
		super();
		this.mgServe = mgServe;
		this.mServe = mServe;
		this.uServe = userve;
	}
	
	@GetMapping(value="/init")
	public ResponseEntity<String> initialTestGroup() {
		User tempUser = uServe.getUserByUsername("DustyDaClown");
		MemoGroup initGroup = new MemoGroup("test group", tempUser);
		mgServe.saveGroup(initGroup);
		return ResponseEntity.status(201).body("Success");		
	}
	
	@PostMapping(value="/blank/{username}")
	public ResponseEntity<String> addEmptyMemoGroup(@RequestBody MemoGroup memoGroup, @PathVariable String username) {
		User userHold = uServe.getUserByUsername(username);
		memoGroup.setUser(userHold);
		mgServe.saveGroup(memoGroup);
		return ResponseEntity.status(201).body("Sussess");		
	}
	
	@GetMapping("/{username}")
    public ResponseEntity<List<MemoGroup>> getGroupsByUser(@PathVariable String username) {
        // You’ll likely want to fetch the user by userId from UserService/UserRepository
		User user = uServe.getUserByUsername(username);
        return ResponseEntity.ok(mgServe.getAllGroupsByUser(user));
    }
	
	@PutMapping("/{groupId}/addmemo")
	public ResponseEntity<MemoGroup> addMemoToGroup(
	        @PathVariable Long groupId,
	        @RequestBody Memo memo) {
	    MemoGroup updatedGroup = mgServe.addMemoToGroup(groupId, memo);
	    return ResponseEntity.ok(updatedGroup);
	}

    @DeleteMapping("/{groupId}")
    public ResponseEntity<?> deleteGroup(@PathVariable Long groupId) {
        mgServe.deleteGroup(groupId);
        return ResponseEntity.ok().build();
    }

}