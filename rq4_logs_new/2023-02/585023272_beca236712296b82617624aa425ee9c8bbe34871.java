package com.spring_boot_momentor;

import java.time.Duration;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

import com.spring_boot_momentor.model.CardVO;
import com.spring_boot_momentor.service.CardService;

@Component
@Controller
public class SeleniumController {
	@Autowired
	CardService service;
	
	private WebDriver driver;
	private WebDriverWait wait;
	private static final String url = "https://www.lottecard.co.kr/app/LPCDADA_V100.lc"; // 크롤링 진행할 페이지 주소 입력

	public void process() {
		System.setProperty("webdriver.chrome.driver",
				"C:\\Program Files\\Google\\Chrome\\ChromeDriver\\chromedriver.exe");
		// 크롬 드라이버 세팅 (드라이버 설치한 경로 입력)

		ChromeOptions options = new ChromeOptions();
		options.addArguments("--disable-popup-blocking"); //팝업안띄움
	    options.addArguments("headless"); //브라우저 안띄움
	    options.addArguments("--disable-gpu"); //gpu 비활성화
	    driver = new ChromeDriver(options);

		try {
			getDataList();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		driver.close(); // 탭 닫기
		driver.quit(); // 브라우저 닫기
	}

	// 데이터 가져오기
	private List<WebElement> getDataList() throws InterruptedException {
		CardVO vo = new CardVO();
		wait = new WebDriverWait(driver, Duration.ofSeconds(3));
		
		driver.get(url); // 브라우저에서 url로 이동한다.
		Thread.sleep(1000); // 강제로 1초 대기
		
		for(int i=0; i<4; i++) {
			driver.findElement(By.xpath("//*[@id=\"btnMore\"]")).click(); // 더보기 버튼을 클릭
			wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath("//*[@id=\"btnMore\"]"))); // 클릭 후 페이지가 로드될 때까지 대기
		}
		
		List<WebElement> elements = driver.findElements(By.cssSelector(".cardList #ajaxCardList li"));
        for (WebElement element : elements) {
            vo.setCardName(element.findElement(By.cssSelector("a .tit")).getText());    
            vo.setCardDes(element.findElement(By.cssSelector("a .txt")).getText());
            vo.setCardImgUrl(element.findElement(By.cssSelector("img")).getAttribute("src"));
            String commonUrl = "https://www.lottecard.co.kr/app/LPCDADB_V100.lc?vtCdKndC=";
            String urlExtraction = element.findElement(By.cssSelector("a")).getAttribute("onclick").substring(7, 20);
            vo.setCardFee(0);
            vo.setCardUrl(commonUrl + urlExtraction);
            vo.setComCtg("lotte");
            service.insertCard(vo);
        }
        

		return elements;
	}

}