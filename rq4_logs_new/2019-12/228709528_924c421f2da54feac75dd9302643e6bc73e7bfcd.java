package com.skilldistillery.giggity;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.skilldistillery.giggity.entities.Job;
import com.skilldistillery.giggity.repositories.JobRepo;

@RunWith(SpringRunner.class)
@SpringBootTest
class JobRepoTest {

	@Autowired
	private JobRepo repo;
	
	@Test
	void test() {
		List<Job> jobs = repo.findByAddress_CityLike("%Denver%");
		assertTrue(jobs.size() > 0);
	}

}