import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

import com.bean.AnotherBean;
import com.bean.EchoBean;


public class EchoBeanTest {

	public static void main(String[] args) {
		
		ApplicationContext ctx = new GenericXmlApplicationContext("classpath:com/bean/echo.xml");
		
		EchoBean bean = ctx.getBean("echo", EchoBean.class);
	    //AnotherBean a = bean.getAnother();
		//System.out.println(a);
	}

}