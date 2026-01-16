package com.hanbit.there.api.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.hanbit.there.api.service.ContactService;
import com.hanbit.there.api.vo.ContactVO;

@RestController
@RequestMapping("/api")
public class ContactController {

	@Autowired
	private ContactService contactService;

	@PostMapping("/contact")
	public Map contactUs(@RequestParam("name") String name,
			@RequestParam("email") String email,
			@RequestParam("title") String title,
			@RequestParam("message") String text) {

		ContactVO contactVO = new ContactVO();
		contactVO.setName(name);
		contactVO.setEmail(email);
		contactVO.setTitle(title);
		contactVO.setText(text);

		contactService.contactUs(contactVO);

		Map result = new HashMap();
		result.put("status", "ok");

		return result;
	}

}