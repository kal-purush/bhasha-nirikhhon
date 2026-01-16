package cz.prague.js.home.restclient;

import cz.prague.js.home.restclient.service.MarketplaceService;
import cz.prague.js.home.restclient.task.CommonSampleTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class RestclientApplication {

	@Autowired
	private CommonSampleTask task;

	public static void main(String[] args) {
		SpringApplication.run(RestclientApplication.class, args);
	}

	@Scheduled(cron = "0 */1 * * * ?")
	public void cronJobSch() throws Exception {
		task.printAllLastFiveMinutesRecords();
	}

}