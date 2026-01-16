package ru.spb.otus.atmDepartmnent;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.spb.otus.atmDepartmnent.atm.service.BanknoteAtm;
import ru.spb.otus.atmDepartmnent.department.AtmDepartment;

import java.util.ArrayList;
import java.util.List;

/**
 * ATM Department
 * Написать приложение ATM Department:
 * • Приложение может содержать несколько ATM
 * • Departmant может собирать сумму остатков со всех ATM
 * • Department может инициировать событие – восстановить состояние всех ATM до начального.
 * (начальные состояния у разных ATM могут быть разными)
 */

@SpringBootApplication
public class AtmApplication {

	private static AtmDepartment atmDepartment;

	@Autowired
	public AtmApplication(AtmDepartment atmDepartment) {
		this.atmDepartment = atmDepartment;
	}

	public static void main(String[] args) {
		SpringApplication.run(AtmApplication.class, args);
        System.out.println("Atms count: "+atmDepartment.getAtms().size());
        System.out.println("First total balance amount: "+atmDepartment.getBalanceAmount());
        System.out.println("Increase banknote count");
        atmDepartment.getAtms().forEach(atm->{
            atm.receive(List.of(1000));
        });
        System.out.println("Second total balance amount: "+atmDepartment.getBalanceAmount());
        System.out.println("Fire event");
        atmDepartment.setInitialState();
        System.out.println("Theard total balance amount: "+atmDepartment.getBalanceAmount());

    }

}