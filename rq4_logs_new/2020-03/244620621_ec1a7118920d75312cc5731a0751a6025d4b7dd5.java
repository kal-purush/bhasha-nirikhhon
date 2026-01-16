package com.liuzhenzhen.cms.dao;

import java.util.List;

import com.liuzhenzhen.cms.entity.Vote;

public interface VoteDao {

	Vote select(Vote vote);

	int insert(Vote vote);

	List<Vote> selects(Integer id);

}