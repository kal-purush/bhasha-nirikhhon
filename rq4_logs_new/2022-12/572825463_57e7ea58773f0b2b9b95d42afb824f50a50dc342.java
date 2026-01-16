package com.serverprogramming.birthdayapp.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.serverprogramming.birthdayapp.exception.FriendNotFoundException;
import com.serverprogramming.birthdayapp.model.Friend;
import com.serverprogramming.birthdayapp.repository.FriendRepository;

import java.util.List;

@RestController
@CrossOrigin("http://localhost:3000")

public class FriendController {
	
	@Autowired
	private FriendRepository friendRepository;
	
	@PostMapping("/friend")
	Friend newFriend(@RequestBody Friend newFriend) {
		return friendRepository.save(newFriend);
	}
	
	@GetMapping("/friends")
	List<Friend> getAllFriends(){
		return friendRepository.findAll();
	}
	
	@GetMapping("/friend/{id}")
	Friend getFriendsById(@PathVariable Long id) {
		return friendRepository.findById(id)
				.orElseThrow(()->new FriendNotFoundException(id));
	}
	
	@PutMapping("/friend/{id}")
	Friend updateFriend(@RequestBody Friend newFriend, @PathVariable Long id) {
		return friendRepository.findById(id)
				.map(friend -> {
					friend.setName(newFriend.getName());
					friend.setLastName(newFriend.getLastName());
					friend.setDate(newFriend.getDate());
					friend.setEmail(newFriend.getEmail());
					
					return friendRepository.save(friend);
					
				}).orElseThrow(()->new FriendNotFoundException(id));
		
	}
	
	@DeleteMapping("/friend/{id}")
	String deleteFriend(@PathVariable Long id) {
		if(!friendRepository.existsById(id)) {
			throw new FriendNotFoundException(id);
		}
		friendRepository.deleteById(id);
		return "Friend with id " + id + " had been deleted successfully.";
	}
	
}
