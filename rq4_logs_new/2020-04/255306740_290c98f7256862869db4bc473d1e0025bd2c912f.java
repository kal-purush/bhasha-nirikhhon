import org.springframework.beans.Person;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Application {
	public static void main(String[] args) {
		ApplicationContext context =
				new ClassPathXmlApplicationContext("classpath*:application.xml");
		Person person = context.getBean("person", Person.class);
		System.out.println(person.getName());
	}
}