package com.accelaero.aeroinventory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.accelaero.aeroinventory.dao.mongo.UserRepository;
import com.accelaero.aeroinventory.model.document.User;
import com.accelaero.aeroinventory.model.rdbms.CurrencyExchangeRate;
import com.accelaero.aeroinventory.service.CurrencyService;

@SpringBootApplication
public class SampleAppApplication implements CommandLineRunner {

	@Autowired
	private UserRepository repository;
	
	@Autowired CurrencyService currencyService;

	public static void main(String[] args) {
		SpringApplication.run(SampleAppApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		repository.deleteAll();

		repository.save(new User("Muhammad", "Rikaz", "mrikaz@accelaero.com", "mrikaz", "rikaz"));

		// fetch all Users
		System.out.println("Users found with findAll():");
		System.out.println("-------------------------------");
		for (User user : repository.findAll()) {
			System.out.println(user);
		}
		System.out.println();

		System.out.println("Users found with findByLastName('Muhammad'):");
		System.out.println("--------------------------------");
		for (User user : repository.findByLastName("Muhammad")) {
			System.out.println(user);
		}
		
		System.out.println("--------------------------------");
		System.out.println("--------------------------------");
		System.out.println("--------------------------------");
//		
//		Currency currency = new Currency();
//		currency.setCurrencyCode("USD");
//		currency.setCurrencyName("United States Dollar");
//		currency.setStatus("ACT");
//		
//		
//		
//		CurrencyExchangeRate currencyExchangeRate = new CurrencyExchangeRate();
//		currencyExchangeRate.setEffectiveFrom(new Date());
//		currencyExchangeRate.setEffectiveTo(new Date());
//		currencyExchangeRate.getEffectiveTo().setYear(2020);
//		currencyExchangeRate.setCurrencyCode("USD");
//		currencyExchangeRate.setExRateFromBase(new BigDecimal("0.27"));
//		currencyExchangeRate.getExRateFromBase().setScale(4);
//		currencyExchangeRate.setExRateToBase(new BigDecimal("3.67"));
//		currencyExchangeRate.getExRateToBase().setScale(4);
//		currencyExchangeRate.setStatus("ACT");
//		
//		currency.setCurrencyExchangeRates(new HashSet<>());
//		currency.getCurrencyExchangeRates().add(currencyExchangeRate);
//		
//		currencyService.saveCurrency(currency);
		//currencyService.saveCurrencyExchangeRate(currencyExchangeRate);
		
		System.out.println("--------------------------------");
		System.out.println("--------------------------------");
		System.out.println("--------------------------------");
		
		CurrencyExchangeRate exRate = currencyService.getExchangeRate("USD");
		if (exRate != null) {
			System.out.println(exRate.getCurrencyCode());
			System.out.println(exRate.getEffectiveFrom());
			System.out.println(exRate.getEffectiveTo());
			System.out.println(exRate.getExRateFromBase());
			System.out.println(exRate.getExRateToBase());
			System.out.println(exRate.getStatus());

		} else {
			System.out.println("Currency Null");
		}
		
		

	}

}