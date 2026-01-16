package utils;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.testng.TestNG;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlInclude;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import com.aventstack.extentreports.MediaEntityBuilder;
import com.aventstack.extentreports.Status;

import static base.BaseTestForParallelExecution.*;

public class CommonUtils {

	public static String encodeString(String stringToEncode) {
		String encodedString = Base64.getEncoder().encodeToString(stringToEncode.getBytes());
		return encodedString;
	}

	public static String decodeString(String stringToDecode) {
		String decodedString = new String(Base64.getDecoder().decode(stringToDecode));
		return decodedString;
	}

	public static void hightlightElement(WebElement element) {
		JavascriptExecutor js = (JavascriptExecutor) getDriver();
		js.executeScript("arguments[0].style.border='3px solid blue'", element);
	}

	public static void removeHighlight(WebElement element) {
		JavascriptExecutor js = (JavascriptExecutor) getDriver();
		js.executeScript("arguments[0].style.border=''", element);
	}

	public static void moveToElementWithActionsClass(WebElement element) {
		Actions actions = new Actions(getDriver());
		actions.moveToElement(element).click().build().perform();
	}

	public static List<String> testClassName = new ArrayList<String>();
	public static List<String> testCaseMethod = new ArrayList<String>();

	public static void readExcel() {
		try {
			XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(
					System.getProperty("user.dir") + File.separator + "src" + File.separator + "test" + File.separator
							+ "resources" + File.separator + "TestData" + File.separator + "testData.xlsx"));
			XSSFSheet sheet = workbook.getSheetAt(0);
			int numberOfRowsPresent = sheet.getLastRowNum() + 1;
			int numberOfColumnsPresent = sheet.getRow(0).getPhysicalNumberOfCells();
			System.out.println("Number of rows present : " + numberOfRowsPresent + " , Number of columns present : "
					+ numberOfColumnsPresent);
			int count = 1;
			for (Row row : sheet) {
				if (count == 1) {
					count++;
					continue;
				}
				if (row.getCell(0).getBooleanCellValue() == true) {
					testClassName.add(row.getCell(1).getStringCellValue());
					testCaseMethod.add(row.getCell(2).getStringCellValue());
				}
			}
			System.out.println(testClassName + " , " + testCaseMethod);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void runTest(boolean parallel) {
		// read from excel
		TestNG testNG = new TestNG();
		XmlSuite suite = new XmlSuite();
		suite.setName("Dynamic Suite");
		if (parallel) {
			suite.setParallel(XmlSuite.ParallelMode.METHODS);
			suite.setThreadCount(testClassName.size());
		} else {
			suite.setThreadCount(1);
		}
		suite.getListeners().add("utils.ExtentReportListener");
		XmlTest test = new XmlTest(suite);
		test.setName("Dynamic Test");
		for (int i = 0; i < testClassName.size(); i++) {
			XmlClass testClass = new XmlClass("testCases." + testClassName.get(i));
			test.getClasses().add(testClass);
			XmlInclude include = new XmlInclude(testCaseMethod.get(i));
			testClass.getIncludedMethods().add(include);
			testNG.setXmlSuites(Collections.singletonList(suite));
		}
//		System.out.println("Suite created dynamically : "+ suite.toXml());
		testNG.run();
	}

	public static String captureBase64ScreenshotAndReturnPath() {
		return ((TakesScreenshot) getDriver()).getScreenshotAs(OutputType.BASE64);
	}

	public static String takeScreenShotAndReturnPath() {
		File src = null;
		File desc = new File(filePath.get() + File.separator + "img_" + System.currentTimeMillis() + ".png");
		try {
			src = ((TakesScreenshot) getDriver()).getScreenshotAs(OutputType.FILE);
			FileUtils.copyFile(src, desc);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return desc.getAbsolutePath();
	}

	public String getTextFromClipboard() throws UnsupportedFlavorException, IOException {
		Toolkit toolkit = Toolkit.getDefaultToolkit();
		Clipboard clipboard = toolkit.getSystemClipboard();
		String text = (String) clipboard.getData(DataFlavor.stringFlavor);
		return text;
	}

	public int getRandomNumber() {

		Supplier<Integer> random = new Supplier<Integer>() {
			public Integer get() {
				int max = 10, min = 1;
				return (int) (Math.random() * (max - min + 1) + min);
			}
		};
		return random.get();
	}

	public void refreshWebpage(WebDriver driver) {
		driver.get(driver.getCurrentUrl());
	}

	// window.scrollTo and window.scroll => will scroll from top of page from(0,0)
	// window.scrollBy => will scroll from current position of webPage

	/*
	 * public void scrollBycoordinates(int x, int y, WebDriver driver) { js =
	 * (JavascriptExecutor) driver; js.executeScript("window.scrollBy(" + x + "," +
	 * y + ")", new Object() { }); }
	 * 
	 * public void scroll_To_Botton_Of_Page(WebDriver driver) { js =
	 * (JavascriptExecutor) driver;
	 * js.executeAsyncScript("window.scrollTo(0,document.body.scrollHeight)"); }
	 * 
	 * public void scroll_To_Element(WebDriver driver, WebElement element) { js =
	 * (JavascriptExecutor) driver;
	 * js.executeScript("arguments[0].scrollIntoView(true)", element); }
	 * 
	 * public void wait_Until_Page_Is_Loaded(WebDriver driver) { js =
	 * (JavascriptExecutor) driver;
	 * js.executeAsyncScript("return document.readystate").toString().equals(
	 * "complete"); }
	 */

	public String getDateBasedOnTodaysDate(boolean upcomingDays, long howManyDaysBeforeOrAfter,
			boolean monthInLetterFormat, boolean firstThreeLetterOfMonth) {

		String months[] = { "", "January", "February", "March", "April", "May", "June", "July", "August", "September",
				"November", "December" };

		LocalDate today = LocalDate.now();
		LocalDate returningDate;
		String month = "";
		if (upcomingDays) {
			returningDate = today.plusDays(howManyDaysBeforeOrAfter);
		} else {
			returningDate = today.minusDays(howManyDaysBeforeOrAfter);
		}

		if (monthInLetterFormat && firstThreeLetterOfMonth) {
			month = months[returningDate.getMonthValue()].substring(0, 3);
		} else {
			month = months[returningDate.getMonthValue()];
		}

		String monthIntegerValue = today.toString().substring(5, 7);
		return returningDate.toString().replace(monthIntegerValue, month);

	}

	@SuppressWarnings({ "deprecation", "unchecked" })
	public String updateExistingJson() throws java.io.FileNotFoundException, IOException, ParseException {
		JSONParser jsonParser = new JSONParser();
		File jsonFile = new File("src/main/resources/Sample_Json.json");
		Object obj = jsonParser.parse(new FileReader(jsonFile));
		JSONObject jsonObject = (JSONObject) obj;
		jsonObject.put("Today date", getDateBasedOnTodaysDate(false, 0, true, false));
		FileWriter fw = new FileWriter(jsonFile);
		fw.write(jsonObject.toString());
		fw.close();
		return FileUtils.readFileToString(jsonFile);
	}

	public static String getCurrentDateAndTime() {
		LocalDateTime currentTime = LocalDateTime.now();
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH:mm:ss");
		String result = currentTime.format(dateTimeFormatter).replace(" ", "");
		return result.replace(":", "");
	}

	public static void attachScreenshotInReport(boolean base64, String nameForScreenShot) {
		if (base64) {
			try {
				Thread.sleep(2000);
			} catch (Exception ignored) {

			}
			ExtentReportListener.logger.get().info(nameForScreenShot);
			ExtentReportListener.logger.get().log(Status.INFO, MediaEntityBuilder
					.createScreenCaptureFromBase64String(captureBase64ScreenshotAndReturnPath()).build());
		} else {
			try {
				Thread.sleep(2000);
			} catch (Exception ignored) {

			}
			ExtentReportListener.logger.get().info(nameForScreenShot);
			ExtentReportListener.logger.get().log(Status.INFO,
					MediaEntityBuilder.createScreenCaptureFromPath(takeScreenShotAndReturnPath()).build());
		}
	}

	public static String createFolder(String path) {
		File folder = new File(path + "_" + getCurrentDateAndTime());
		if (folder.mkdir()) {
			return folder.getAbsolutePath();
		}
		return path;
	}

}