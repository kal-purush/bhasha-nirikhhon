package base;

import org.apache.log4j.Logger;
import appium.*;

import io.appium.java_client.android.AndroidElement;

public class BaseElement extends AppTest {
	public static String pageDec;
	public static String btnName;
	public Logger logger = Logger.getRootLogger();
	public BaseElement(String pageDec) {
		BaseElement.pageDec = pageDec;
		// TODO Auto-generated constructor stub
	}
	
	
   
	/**
	 * @描述：使用ID找到一个元素
	 * @作者：wbc
	 * @param id 元素id
	 * @param btnName 按钮名称
	 * @param pageDec 页面名称
	 * @return 返回Android 元素类型
	 * @throws Exception
	 */
	public AndroidElement initElement (String id,String pageDec,String btnName) {
		BaseElement.btnName = btnName;
		BaseElement.pageDec = pageDec;
		return driver.findElementById(id);
	}
	
	public String getBtnName() {
		return btnName;
	}
	
	public String getPageDec() {
		return pageDec;
	}

}
