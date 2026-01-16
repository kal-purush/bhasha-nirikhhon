package currency.impl;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import currency.Currency;
import currency.CurrencyService;
import currency.dto.CurrencyItemDTOMono;
import org.jsoup.Jsoup;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

public class CurrencyServiceImplMono implements CurrencyService {
        String url = "https://api.monobank.ua/bank/currency";
        String response;

        {
            try {
                response = Jsoup.connect(url).ignoreContentType(true).get().body().text();
            } catch (IOException e) {
                System.out.println("Помилка під час запиту валюти");
            }
        }
        Type type = TypeToken.getParameterized(List.class, CurrencyItemDTOMono.class).getType();
        List<CurrencyItemDTOMono> list = new Gson().fromJson(response, type);
        String strBuy = "";
        String strSale = "";

        int currencyCode;
        @Override
        public double getRateBuy(Currency ccy) {
            switch (ccy) {
                case USD:
                    currencyCode = 840;
                    break;
                case EUR:
                    currencyCode = 978;
                    break;
                case UAH:
                    currencyCode = 980;
                    break;
            }

            strBuy = list.stream()
                    .filter(c -> c.getCurrencyCodeA() == currencyCode)
                    .filter(c -> c.getCurrencyCodeB() == 980)
                    .map(c -> String.valueOf(c.getRateBuy()))
                    .findFirst()
                    .orElseThrow();

            return Double.parseDouble(strBuy);
        }

        @Override
        public double getRateSale(Currency ccy) {
            strSale = list.stream()
                    .filter(c -> c.getCurrencyCodeA() == currencyCode)
                    .filter(c -> c.getCurrencyCodeB() == 980)
                    .map(c -> String.valueOf(c.getRateSell()))
                    .findFirst()
                    .orElseThrow();
            return Double.parseDouble(strSale);
        }
}