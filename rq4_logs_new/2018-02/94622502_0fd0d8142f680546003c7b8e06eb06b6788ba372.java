package edu.unifi.tap.exambooking.bdd;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.MalformedURLException;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.openqa.selenium.support.ui.Select;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;
import edu.unifi.tap.exambooking.exception.ExamsNotFoundException;
import edu.unifi.tap.exambooking.model.Exam;
import edu.unifi.tap.exambooking.model.Student;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class HomeControllerWebDriverIT {

	@Autowired
	private WebDriver webDriver;

	@LocalServerPort
	private int port;

	private RegisterStudentPage registerStudentPage;

	private SuccessPage successPage;

	private ErrorPage errorPage;
	
	private Exam exam;
	private Student succesfullyCreatedUser;
	private Student fakeUser;
	
	private String expectedMessage;
	private String expectedErrorMessage;

	private static final String STUDENT_REGISTRATION_SUCCESS_MSG = "Student #x# correctly registered for exam #y# !";
	private static final String INVALID_STUDENT_ERROR_MSG = "Something wrong happened storing Student data: missing field value";
	
	@TestConfiguration
	static class WebDriverConfiguration {
		@Bean
		public WebDriver getWebDriver() {
			return new HtmlUnitDriver();
		}
	}
	
	@Before
	public void prepare() {
		AbstractPage.port = port;
		exam  = new Exam(0L, "CSZ", "Codici e sicurezza", null, "Aula 101");
		succesfullyCreatedUser = new Student(0L, "AFirstname", "ASurname", "AFirstname.ASurname@email.it", "7320382");
		fakeUser = new Student(1L, "fakeUserFirstname", "", "fakeUserFirstname@email.it", "1111111");
	}
	
	static final Logger LOGGER = Logger.getLogger(HomeControllerWebDriverIT.class);
	
	@BeforeClass
	public static void beforeTest(){
		LOGGER.info("");
		LOGGER.info("+++++ START SELENIUM INTEGRATION TESTS +++++");
		LOGGER.info("");
	}
	
	@AfterClass
	public static void afterTest(){
		LOGGER.info("");
		LOGGER.info("----- END SELENIUM INTEGRATION TESTS -----");
		LOGGER.info("");
	}
	

	@Test
	public void missingStudentFieldDisplaysError() {
		
		expectedErrorMessage = INVALID_STUDENT_ERROR_MSG;
		
		registerStudentPage = RegisterStudentPage.to(webDriver);
		errorPage = registerStudentPage.createMessage(ErrorPage.class, fakeUser.getFirstName(),fakeUser.getLastName(),fakeUser.getEmail(),fakeUser.getIdNumber());
		assertThat(errorPage.getErrorMessage()).isEqualTo(expectedErrorMessage);

	}
	
	@Test
	public void successfullyRegisterStudent(){
				
		registerStudentPage = RegisterStudentPage.to(webDriver);
		Select dropdown = new Select(webDriver.findElement(By.id("examId")));
		dropdown.selectByIndex(exam.getExamId().intValue());

		expectedMessage = STUDENT_REGISTRATION_SUCCESS_MSG
				.replace("#x#", succesfullyCreatedUser.getLastName())
				.replace("#y#", exam.getExamName());
		
		successPage = registerStudentPage.createMessage(SuccessPage.class, succesfullyCreatedUser.getFirstName(),succesfullyCreatedUser.getLastName(),succesfullyCreatedUser.getEmail(),succesfullyCreatedUser.getIdNumber());
		assertThat(successPage.getMessage()).isEqualTo(expectedMessage);
	}
	
	
	@Test
	public void getHomePage() throws ExamsNotFoundException, InterruptedException, MalformedURLException{
		AbstractPage.get(webDriver, "");
		System.out.println("------------------------");
		System.out.println(webDriver.getTitle());
		System.out.println("------------------------");
		System.out.println(webDriver.getPageSource());
		System.out.println("------------------------");
	}
}