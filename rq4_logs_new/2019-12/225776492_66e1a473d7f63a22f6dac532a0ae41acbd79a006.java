package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@RestController
	public class DockerController {

		@Autowired
		MongoTemplate tmpl;

		@Autowired
		PeopleRepository repo;

		@RequestMapping("/")
		public String index() {
			People pe = new People();
			pe.setName("test" + repo.count());
			pe.setAge((int) repo.count());
			repo.save(pe);

			pe = repo.findOneByName("test6");
			pe.setAge(pe.getAge() + 1);
			repo.save(pe);

			tmpl.updateFirst(Query.query(Criteria.where("name").is("test8")), Update.update("name", "test8").inc("age", 1),
					People.class);

			return "Hello Docker!" + tmpl.getDb().getName() + repo.count();
		}
	}
}