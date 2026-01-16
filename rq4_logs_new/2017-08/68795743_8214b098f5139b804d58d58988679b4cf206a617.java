package java8.ch04.ex10;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;

public class AccessURL extends Application {

	@Override
	public void start(Stage stage) {

		BorderPane pane = new BorderPane();
		// browser
		String location = "https://www.google.co.jp/";
		WebView browser = new WebView();
		WebEngine engine = browser.getEngine();
		engine.load(location);
		pane.setCenter(browser);

		// URLバー
		TextField textbox = new TextField();
		textbox.setOnAction(e -> {
			engine.load(textbox.getText());
		});
		pane.setTop(textbox);
		textbox.textProperty().bind(engine.locationProperty());
		// 戻るボタン
		Button backButton = new Button("back");
		backButton.setOnAction(e -> {
			try {
				engine.getHistory().go(-1);
			} catch (IndexOutOfBoundsException ex) {
				// 戻れないなら何もしない
			}
		});
		pane.setLeft(backButton);
		// 進むボタン
		Button nextButton = new Button("next");
		nextButton.setOnAction(e -> {
			try {
				engine.getHistory().go(1);
			} catch (IndexOutOfBoundsException ex) {
				// 進めないなら何もしない
			}
		});
		pane.setRight(nextButton);

		stage.setScene(new Scene(pane, 800, 450));
		stage.show();

	}

	public static void main(String[] args) {
		launch(args);
	}
}