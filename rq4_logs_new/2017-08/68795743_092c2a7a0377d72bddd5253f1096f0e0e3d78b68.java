package java8.ch04.ex05;

import static javafx.beans.binding.Bindings.*;

import java.util.function.BiFunction;
import java.util.function.Function;

import javafx.application.Application;
import javafx.beans.value.ObservableValue;
import javafx.scene.control.Button;
import javafx.stage.Stage;

public class ObservableClass extends Application {

	public static <T, R> ObservableValue<R> observe(Function<T, R> f, ObservableValue<T> t) {
		return createObjectBinding(() -> f.apply(t.getValue()));
	}

	public static <T, U, R> ObservableValue<R> observe(BiFunction<T, U, R> f, ObservableValue<T> t,
			ObservableValue<U> u) {
		return createObjectBinding(() -> f.apply(t.getValue(), u.getValue()));
	}

	@Override
	public void start(Stage primaryStage) throws Exception {
		Button larger = new Button();
		//larger.disabledProperty().bind(observe(t -> t >= 100, gauge.widthProperty()));

	}

	public static void main(String[] args) {
		launch(args);

	}

}