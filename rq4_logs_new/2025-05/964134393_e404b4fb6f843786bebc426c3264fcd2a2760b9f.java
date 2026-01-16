package com.example.controller;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.model.Contact;
import com.example.model.Memo;
import com.example.model.User;
import com.example.service.MemoService;
import com.example.service.UserService;

@ComponentScan(basePackages = {
	    "com.technicalkeeda"
	})
@RestController
@CrossOrigin(origins="*")
@RequestMapping(value="/memo")
public class MemoController {
	
	UserService uServe;
	MemoService mServe;
	
	@Autowired
	public MemoController(UserService uServe, MemoService mServe) {
		super();
		this.uServe = uServe;
		this.mServe = mServe;
	}
	
	@GetMapping(value="/init")
	public ResponseEntity<String> initiateMockMemoData() {
		User testUser = uServe.getUserByUsername("DustyDaClown");
		Memo testMemo = new Memo("Test Title", "This memo has content. I have no idea what the maximum character count is, but we haven't reached it yet", testUser);
		mServe.saveMemo(testMemo);
		return ResponseEntity.status(201).body("Success");
	}
	
	//get single contact by unique id
	@GetMapping(value="/{id}")
	public ResponseEntity<Memo> getContact(@PathVariable(name="id") int memoId) {
		return ResponseEntity.status(201).body(mServe.getMemoById(memoId));
	}
	
	//get all contacts joined by user
	@GetMapping(value="/all/{username}")
	public ResponseEntity<List<Memo>> getAllContacts(@PathVariable(name="username") String username) {
		List<Memo> memoList = mServe.getMemosByUserUserName(username);
		return ResponseEntity.status(201).body(memoList);		
	}
	
	//add new contact to specified user via username
		/*JSON format
	    "memoTitle": "Christina O'Neal",
	    "dob": "1968-11-09"
	    */
	@PostMapping(value="/{username}")
	public ResponseEntity<String> addNewMemo(@RequestBody Memo memo, @PathVariable(name="username") String username) {
		System.out.println(memo);
		User memoOwner = uServe.getUserByUsername(username);
		Optional<Memo> memoOpt = Optional.ofNullable(memo);
		memoOpt.get().setUser(memoOwner);
		if(!memoOpt.isPresent()) {
			return ResponseEntity.badRequest().build();
		}
		mServe.saveMemo(memo);
		return ResponseEntity.status(202).body("Success");
	}
	
	@PutMapping(value="/edit/contact/{id}")
	public ResponseEntity<String> editContact(@PathVariable(name="id") int memoId) {
		Optional<Memo> memoOpt = Optional.ofNullable(mServe.getMemoById(memoId));
		System.out.println(memoOpt.get());
		if(memoOpt.isPresent()) {
			//Memo tempMemo = 
			//mServe.saveMemo();
			return ResponseEntity.status(201).body("Success");
		}
		return ResponseEntity.badRequest().build();
	}

}