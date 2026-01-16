package pl.dgrebowiec.counterwords;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = CounterWordsApplication.class)
@WebAppConfiguration
public class CounterWordsApplicationTestsd {

	@Test
	public void contextLoads() {
	}

}