package cn.lixing.zqProject.AssorderManage.Element;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import cn.lixing.zqProject.Pages.ParentPage;

public class AssorderElementObject {
	private WebElement assorderListElement;
	private WebElement labelManageBtnElement;
	private WebElement AddlabelBtnElement;
	private WebElement labelContextElement;
	private WebElement labelStarsSelectElement;
	private WebElement StarsElement;
	private WebElement createBtnElement;
	private WebElement acceptBtnElement;
	
	private ParentPage page;
	private AssorderElementXPathData xPathData;
	private WebDriver driver;
	private ApplicationContext context;
	
	public AssorderElementObject() {
		context=new ClassPathXmlApplicationContext("AppctionContext.xml");
		page=(ParentPage) context.getBean("parent");
		xPathData=new AssorderElementXPathData();
		driver=page.startBrowse();
	}

	public WebElement getAssorderListElement() {
		assorderListElement=page.getWebElement(xPathData.getAssorderListXpath());
		return assorderListElement;
	}

	public void setAssorderListElement(WebElement assorderListElement) {
		this.assorderListElement = assorderListElement;
	}

	public WebElement getLabelManageBtnElement() {
		labelManageBtnElement=page.getWebElement(xPathData.getLabelManageBtnXpath());
		return labelManageBtnElement;
	}

	public void setLabelManageBtnElement(WebElement labelManageBtnElement) {
		this.labelManageBtnElement = labelManageBtnElement;
	}

	public WebElement getAddlabelBtnElement() {
		AddlabelBtnElement=page.getWebElement(xPathData.getAddlabelBtnXpath());
		return AddlabelBtnElement;
	}

	public void setAddlabelBtnElement(WebElement addlabelBtnElement) {
		AddlabelBtnElement = addlabelBtnElement;
	}

	public WebElement getLabelContextElement() {
		labelContextElement=page.getWebElement(xPathData.getLabelContextXpath());
		return labelContextElement;
	}

	public void setLabelContextElement(WebElement labelContextElement) {
		this.labelContextElement = labelContextElement;
	}

	public WebElement getLabelStarsSelectElement() {
		labelStarsSelectElement=page.getWebElement(xPathData.getLabelStarsSelectXpath());
		return labelStarsSelectElement;
	}

	public void setLabelStarsSelectElement(WebElement labelStarsSelectElement) {
		this.labelStarsSelectElement = labelStarsSelectElement;
	}

	public WebElement getStarsElement() {
		StarsElement=page.getWebElement(xPathData.getStarsXpath());
		return StarsElement;
	}

	public void setStarsElement(WebElement starsElement) {
		StarsElement = starsElement;
	}

	public WebElement getCreateBtnElement() {
		createBtnElement=page.getWebElement(xPathData.getCreateBtnXpath());
		return createBtnElement;
	}

	public void setCreateBtnElement(WebElement createBtnElement) {
		this.createBtnElement = createBtnElement;
	}

	public WebDriver getDriver() {
		return driver;
	}

	public void setDriver(WebDriver driver) {
		this.driver = driver;
	}

	public ParentPage getPage() {
		return page;
	}

	public void setPage(ParentPage page) {
		this.page = page;
	}

	public WebElement getAcceptBtnElement() {
		acceptBtnElement=page.getWebElement(xPathData.getAcceptBtnXpath());
		return acceptBtnElement;
	}

	public void setAcceptBtnElement(WebElement acceptBtnElement) {
		this.acceptBtnElement = acceptBtnElement;
	}
	
	
}