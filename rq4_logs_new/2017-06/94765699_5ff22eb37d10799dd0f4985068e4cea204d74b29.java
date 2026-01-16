package org.eko.BusinessJobs;

import java.util.List;

import javax.transaction.Transactional;

import org.eko.entity.SimpliJob;
import org.eko.repository.SimpliJobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

@Transactional
@Service
public class Job1 {

	@Autowired
	private SimpliJobRepository simpliJobRepository;
	
	public void doPerform()
	{
		Sort sortID=new Sort(new Sort.Order(Sort.Direction.DESC,"id"));
		
		List<SimpliJob> jobsDescID=simpliJobRepository.findAll(sortID);
		for (SimpliJob simpliJob : jobsDescID) {
			System.out.println(simpliJob);
		}
	}
}