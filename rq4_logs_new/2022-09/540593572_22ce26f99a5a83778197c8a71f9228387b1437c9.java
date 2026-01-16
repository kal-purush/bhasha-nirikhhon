package com.payroll;

import com.payroll.common.Money;
import com.payroll.common.MoneyRate;
import com.payroll.domain.*;
import com.payroll.domain.classification.CommissionedClassification;
import com.payroll.domain.classification.HourlyClassification;
import com.payroll.domain.classification.SalariedClassification;
import com.payroll.domain.scheduler.BiWeeklyScheduler;
import com.payroll.domain.scheduler.MonthlyScheduler;
import com.payroll.domain.scheduler.WeeklyScheduler;
import com.payroll.infrastructure.OptionalUnion;

import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Simple sample payroll App mixing OOP and Functional paradigms.
 */
public class App {

    public static void main(String[] args) {
        new App().run();
    }

    private void run() {
        //Composition of objects
        Payroll payroll = new Payroll(this.createEmployees());

        //Let Objects do their work
        payroll.run(LocalDate.of(2022, 9, 23));
    }

    private List<Employee> createEmployees() {
        return List.of(
                new Employee("Joe", new HourlyClassification(this.createTimeCards(), this.perHour(10)), new WeeklyScheduler()),
                new Employee("Rick", new SalariedClassification(this.getFlatSalary()), new MonthlyScheduler()),
                new Employee("Jill", new CommissionedClassification(this.createSaleReceipts(), this.commission()), new BiWeeklyScheduler(), this.getUnionAffiliation())
        );
    }

    private List<TimeCard> createTimeCards() {
        return List.of(
                TimeCard.of(LocalDate.of(2022, 9, 21), Duration.ofHours(9)),
                TimeCard.of(LocalDate.of(2022, 9, 22), Duration.ofHours(9))
        );
    }

    private Money getFlatSalary() {
        return Money.of(5200);
    }

    private List<SaleReceipt> createSaleReceipts() {
        return List.of(
                SaleReceipt.of(LocalDate.of(2022, 9, 22), Money.of(1500))
        );
    }

    private CommissionRate commission() {
        return CommissionRate.of(Money.of(5200), 0.5);
    }

    private OptionalUnion getUnionAffiliation() {
        return OptionalUnion.of(new Union(MoneyRate.hourly(Money.of(30))));
    }

    private MoneyRate perHour(double amount) {
        return MoneyRate.hourly(Money.of(amount));
    }
}