package com.example.demo;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.ComboBox;
import javafx.scene.image.Image;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.json.JSONObject;

public class HelloApplication extends Application {
    private static final String API_URL = "https://api.freecurrencyapi.com/v1/latest";
    private static final String API_KEY = "fca_live_XPcGann98vQ03TN72CcoOZNaqKXr0Be6wmBVPYXD";

    @Override
    public void start(Stage stage) {
        stage.setTitle("Currency Converter");

        Label label_money = new Label("Enter the amount");
        TextField text_money = new TextField();
        Label label_currency_from = new Label("From currency");
        ComboBox<String> fromCurrencyComboBox = new ComboBox<>();
        Label label_currency_to = new Label("To currency");
        ComboBox<String> toCurrencyComboBox = new ComboBox<>();

        Label resultLabel = new Label();

        try {
            Map<String, Double> currencies = getCurrencyRates();
            fromCurrencyComboBox.getItems().addAll(currencies.keySet());
            toCurrencyComboBox.getItems().addAll(currencies.keySet());
        } catch (Exception e) {
            resultLabel.setText("Error loading currency data.");
        }

        Button converter = new Button("Convert");

        converter.setOnAction(e -> {
            try {
                double amount = Double.parseDouble(text_money.getText());
                String fromCurrency = fromCurrencyComboBox.getValue();
                String toCurrency = toCurrencyComboBox.getValue();
                if (fromCurrency == null || toCurrency == null) {
                    resultLabel.setText("Select the currencies to convert.");
                    return;
                }
                double result = convertCurrency(amount, fromCurrency, toCurrency);
                resultLabel.setText(String.format("%.2f %s = %.2f %s", amount, fromCurrency, result, toCurrency));
            } catch (NumberFormatException ex) {
                resultLabel.setText("Enter the correct amount value.");
            } catch (Exception ex) {
                resultLabel.setText("Error: " + ex.getMessage());
            }
        });

        VBox vbox = new VBox(10, label_money, text_money, label_currency_from, fromCurrencyComboBox, label_currency_to, toCurrencyComboBox, converter, resultLabel);
        Scene scene = new Scene(vbox, 300, 250);
        stage.setScene(scene);
        stage.show();
    }

    private double convertCurrency(double amount, String fromCurrency, String toCurrency) {
        try {
            Map<String, Double> currencies = getCurrencyRates();
            if (!currencies.containsKey(fromCurrency) || !currencies.containsKey(toCurrency)) {
                System.out.println("Invalid currency code.");
                return 0;
            }

            double fromRate = currencies.get(fromCurrency);
            double toRate = currencies.get(toCurrency);
            double amountInBaseCurrency = amount / fromRate;
            return amountInBaseCurrency * toRate;
        } catch (Exception e) {
            System.out.println("Error when converting currency: " + e.getMessage());
            return 0;
        }
    }

    private Map<String, Double> getCurrencyRates() {
        Map<String, Double> currencyRates = new HashMap<>();

        try {
            String urlString = API_URL + "?apikey=" + API_KEY;
            URL url = new URL(urlString);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder response = new StringBuilder();
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            JSONObject jsonResponse = new JSONObject(response.toString());
            JSONObject rates = jsonResponse.getJSONObject("data"); // Получаем объект с курсами валют

            for (String currencyCode : rates.keySet()) {
                double rate = rates.getDouble(currencyCode);
                currencyRates.put(currencyCode, rate);
            }

        } catch (IOException e) {
            System.out.println("Error connecting to the API: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("A common mistake: " + e.getMessage());
        }

        return currencyRates;
    }

    public static void main(String[] args) {
        launch();
    }
}