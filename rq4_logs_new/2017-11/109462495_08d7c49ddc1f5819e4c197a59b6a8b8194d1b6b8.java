package com.nuc.zjy.qa.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.nuc.zjy.qa.bean.EntityType;
import com.nuc.zjy.qa.bean.HostHolder;
import com.nuc.zjy.qa.bean.Msg;
import com.nuc.zjy.qa.bean.Question;
import com.nuc.zjy.qa.service.FollowService;
import com.nuc.zjy.qa.service.QuestionService;

/**
 * @项目名称：QA
 * @类名称：FollowController @类描述：关注
 *
 * @author 赵建银
 * @date 2017年11月16日
 * @time 上午8:39:15
 * @version 1.0
 */
@Controller
public class FollowController {

	@Autowired
	FollowService followService;

	@Autowired
	QuestionService questionService;

	@Autowired
	HostHolder hostHolder;

	@RequestMapping(value = "/followUser")
	@ResponseBody
	public Msg follow(@RequestParam("userId") int userId) {
		Msg msg = new Msg();
		if (hostHolder.getUser() == null) {
			msg.setCode(999);
			return msg;
		}

		boolean ret = followService.follow(hostHolder.getUser().getId(), userId, EntityType.ENTITY_USER);
		msg.setCode(ret ? 0 : 1);
		msg.add("msg", String.valueOf(followService.getFolloweeCount(userId, EntityType.ENTITY_USER)));
		return msg;

	}

	@RequestMapping(value = "/unfollowUser")
	@ResponseBody
	public Msg unfollow(@RequestParam("userId") int userId) {
		Msg msg = new Msg();
		if (hostHolder.getUser() == null) {
			msg.setCode(999);
			return msg;
		}

		boolean ret = followService.unfollow(hostHolder.getUser().getId(), userId, EntityType.ENTITY_USER);
		msg.setCode(ret ? 0 : 1);
		msg.add("msg", String.valueOf(followService.getFolloweeCount(userId, EntityType.ENTITY_USER)));
		return msg;

	}

	@RequestMapping(value = "/followQuestion")
	@ResponseBody
	public Msg followQuestion(@RequestParam("questionId") int questionId) {
		Msg msg = new Msg();
		if (hostHolder.getUser() == null) {
			msg.setCode(999);
			return msg;
		}

		Question question = questionService.getById(questionId);
		if (question == null) {
			msg.setCode(1);
			msg.add("msg", "问题不存在");
			return msg;
		}

		boolean ret = followService.follow(hostHolder.getUser().getId(), questionId, EntityType.ENTITY_QUESTION);
		Map<String, Object> map = new HashMap<>();
		map.put("headUrl", hostHolder.getUser().getHeadUrl());
		map.put("name", hostHolder.getUser().getName());
		map.put("id", hostHolder.getUser().getId());
		map.put("count", followService.getFollowerCount(questionId, EntityType.ENTITY_QUESTION));
		msg.setCode(ret ? 0 : 1);
		msg.add("msg", map);
		return msg;

	}

	@RequestMapping(value = "/unfollowQuestion")
	@ResponseBody
	public Msg unfollowQuestion(@RequestParam("questionId") int questionId) {
		Msg msg = new Msg();
		if (hostHolder.getUser() == null) {
			msg.setCode(999);
			return msg;
		}
		Question question = questionService.getById(questionId);
		if (question == null) {
			msg.setCode(1);
			msg.add("msg", "问题不存在");
			return msg;
		}
		boolean ret = followService.unfollow(hostHolder.getUser().getId(), questionId, EntityType.ENTITY_QUESTION);
		Map<String, Object> map = new HashMap<>();
		map.put("headUrl", hostHolder.getUser().getHeadUrl());
		map.put("name", hostHolder.getUser().getName());
		map.put("id", hostHolder.getUser().getId());
		map.put("count", followService.getFollowerCount(questionId, EntityType.ENTITY_QUESTION));
		msg.setCode(ret ? 0 : 1);
		msg.add("msg", map);
		return msg;
	}
}