package com.niit.controllers;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.niit.dao.MessageDao;
import com.niit.model.Error;
import com.niit.model.MessagePrivate;
import com.niit.model.Users;

@RestController
public class MessageController {
	@Autowired
	private MessageDao messageDao;

	@RequestMapping(value="/sendmessage",method=RequestMethod.POST)
	public ResponseEntity<?> sendMessage(@RequestBody MessagePrivate message,HttpSession session)
	{
		Users user=(Users) session.getAttribute("user");
		if(user==null)
		{
			Error error=new Error(1,"Unauthorized");
			return new ResponseEntity<Error>(error,HttpStatus.UNAUTHORIZED);
		}
		message.setDate(new Date());
		message.setFromUser(user.getUsername());
		messageDao.sendMessage(message); 
        return new ResponseEntity<Void>(HttpStatus.OK);
	}
	
	@RequestMapping(value="/getmessage/{username}",method=RequestMethod.GET)
	public ResponseEntity<?> getMessages(@PathVariable String username,HttpSession session)
	{
		Users user=(Users) session.getAttribute("user");
		if(user==null)
		{
			Error error=new Error(1,"Unauthorized");
			return new ResponseEntity<Error>(error,HttpStatus.UNAUTHORIZED);
		}
		List<MessagePrivate> messages=messageDao.getMessage(user.getUsername(), username);
		return new ResponseEntity<List<MessagePrivate>>(messages,HttpStatus.OK);
	}
}