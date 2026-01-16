package com.levelup.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.levelup.persist.dao.SequenceDao;
import com.levelup.persist.model.Sequence;
import com.levelup.service.SequenceService;

@Service
public class SequenceServiceImpl implements SequenceService {

	@Autowired
	private SequenceDao sequenceDao;

	@Override
	public void save(Sequence obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Sequence obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public void update(Sequence obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public Sequence find(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNextSequenceId(String seqKey) {
		// TODO Auto-generated method stub
		return 0;
	}

}