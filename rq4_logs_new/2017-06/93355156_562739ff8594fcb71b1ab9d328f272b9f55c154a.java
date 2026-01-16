package nsida.kazey.showcase;

import java.io.File;
import java.io.IOException;

//import org.apache.log4j.Logger;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundImage;
import javafx.scene.layout.BackgroundSize;
import javafx.scene.layout.BorderPane;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.stage.StageStyle;
import jssc.SerialPortException;
import nsida.kazey.showcase.util.AppSettings;
import nsida.kazey.showcase.util.GoodsLoader;
import nsida.kazey.showcase.util.ScaleProvider;
import nsida.kazey.showcase.util.TermoPrinter;
import nsida.kazey.showcase.view.AlarmController;
import nsida.kazey.showcase.view.PackingController;
import nsida.kazey.showcase.view.ShowcaseController;

public class MainApp extends Application {

	private Stage primaryStage;
	private ScaleProvider scaleProvider;
	private TermoPrinter termoPrinter;
	private GoodsLoader goodsLoader;
	private Scene showcaseScene = null;
	private Scene packingScene = null;
	private Scene alarmScene = null;
	private static String appFolder = null;
	private PackingController packingController;
	private AlarmController alController;
	//private static Logger log;
	
	public MainApp() throws IOException {						
		
		AppSettings.loadSettings(getAppSettingsFile());
		
		//log = Logger.getLogger(MainApp.class.getName());
		
		termoPrinter = new TermoPrinter(AppSettings.get("printerPort").trim());
		try {
			termoPrinter.pushLabel();
		} catch (SerialPortException e) {
			e.printStackTrace();
		}
		
		scaleProvider = new ScaleProvider(AppSettings.get("scalePort").trim());
		if(getScaleProvider().isReady())
			getScaleProvider().start();
		
		goodsLoader = new GoodsLoader();
		goodsLoader.Start();
		
	}
	
	public static void main(String[] args) {
		if (args.length == 1)
			MainApp.appFolder = args[0];
		
		launch(args);
	}
	
	@Override
	public void start(Stage primaryStage) {		
			
		initShowcase();
		initPacking();
		//initAlarm();
		
		setCommonProps(primaryStage);		
		setMainScene();	
		showPrimaryStage();
	}
	
	
	private void setCommonProps(Stage primaryStage) {
		this.primaryStage = primaryStage;
		this.primaryStage.setWidth(Screen.getPrimary().getBounds().getWidth());
		this.primaryStage.setHeight(Screen.getPrimary().getBounds().getHeight());		
		this.primaryStage.initStyle(StageStyle.TRANSPARENT);
	}
	
	private void showPrimaryStage() {
		primaryStage.show();
	}

	
	public void setMainScene() {
		primaryStage.setTitle("TouchShowcase");		
		primaryStage.setScene(showcaseScene);	
		primaryStage.setFullScreen(true);
	}

	public void setPackingScene() {
		primaryStage.setTitle("Packing");
		primaryStage.setScene(packingScene);
		primaryStage.setFullScreen(true);
		
		packingController.fillTable(null);
	}

	public void setAlarmScene() {
		primaryStage.setTitle("Alarm");
		primaryStage.setScene(alarmScene);
		primaryStage.setFullScreen(true);
	}
	
	
	private void initShowcase() {
				
		try {
			
			FXMLLoader loader = new FXMLLoader();
			loader.setLocation(MainApp.class.getResource("view/ShowcaseLayout.fxml"));			
			BorderPane page = (BorderPane) loader.load();
			
			page.setBackground(
					new Background(
							new BackgroundImage(
									new Image("file:picts/icons/back.jpg"), null,null,null, 
										new BackgroundSize(100, 100, true, true, true, true))
					)
			);
			
			ShowcaseController scController = loader.getController();
			scController.setMainApplication(this);		
			
			getScaleProvider().AddListener(scController);
			getTermoPrinter().AddListener(scController);
			
			showcaseScene = new Scene(page);			
			
		} catch(IOException e ) {
			e.printStackTrace();
		}
	}

	private void initPacking() {
		
		try {
			
			FXMLLoader loader = new FXMLLoader();
	        loader.setLocation(MainApp.class.getResource("view/PackingLayout.fxml"));
	        AnchorPane page = (AnchorPane) loader.load();
	        
	        packingController = loader.getController();	  
	        packingController.setMainApplication(this);
	        
	        getScaleProvider().AddListener(packingController);
	        getTermoPrinter().AddListener(packingController);
	        
	        packingScene = new Scene(page);
	        
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	private void initAlarm(){
		
		try {
			
			FXMLLoader loader = new FXMLLoader();
			loader.setLocation(MainApp.class.getResource("view/AlarmLayout.fxml"));			
			AnchorPane page = (AnchorPane) loader.load();
						
			alController = loader.getController();
			alController.setMainApplication(this);		
			
			getTermoPrinter().AddListener(alController);
			
			alarmScene = new Scene(page);			
			
		} catch(IOException e ) {
			e.printStackTrace();
		}		
	}
		
	public GoodsLoader getGoodsLoader() {
		return goodsLoader;
	}
	
	public TermoPrinter getTermoPrinter() {
		return termoPrinter;
	}
	
	public Stage getPrimaryStage() {
		return primaryStage;
	}
	
	public String getAppFolder() {
		return appFolder;
	}
	
	public File getAppSettingsFile() throws IOException {
		
		File file = new File(appFolder + "/appSettings/config.xml");
		
		System.out.println(file.toString());
		if (!file.exists())
			if (!file.createNewFile())
				throw new IOException("Не удалось создать файл настроек.");
		
		return file;		
	}

	public ScaleProvider getScaleProvider() {
		return scaleProvider;
	}
	
}