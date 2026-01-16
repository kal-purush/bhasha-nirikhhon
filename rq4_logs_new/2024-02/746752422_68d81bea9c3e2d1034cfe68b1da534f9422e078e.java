package example.spring.service;

import example.spring.model.enams.Gender;
import example.spring.model.Account;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AccountServiceForFun {

    public List<Account> filterAccountsByBalance(List<Account> accounts) {
        return accounts.stream()
                .filter(account -> account.getBalance() > 10000)
                .collect(Collectors.toList());
    }

    public Set<String> getUniqueCountries(List<Account> accounts) {
        return accounts.stream()
                .map(Account::getCountry)
                .collect(Collectors.toSet());
    }

    public boolean hasYoungerThan2000(List<Account> accounts) {
        LocalDate thresholdDate = LocalDate.of(2000, 1, 1);
        return accounts.stream()
                .anyMatch(account -> account.getBirthday().isAfter(thresholdDate));
    }

    public double getBalanceOfMaleAccounts(List<Account> accounts) {
        return accounts.stream()
                .filter(account -> Gender.MALE.equals(account.getGender()))
                .mapToDouble(Account::getBalance)
                .sum();
    }

    public Map<Integer, List<Account>> groupByBirthMonth(List<Account> accounts) {
        return accounts.stream()
                .collect(Collectors.groupingBy(account -> account.getBirthday().getMonthValue()));
    }

    public double getAverageBalanceByCountry(List<Account> accounts, String country) {
        return accounts.stream()
                .filter(account -> country.equals(account.getCountry()))
                .mapToDouble(Account::getBalance)
                .average()
                .orElse(0.0);
    }

    public String getNamesConcatenated(List<List<Account>> accountsLists) {
        return accountsLists.stream()
                .flatMap(List::stream)
                .map(account -> account.getFirstName() + " " + account.getLastName())
                .collect(Collectors.joining(", "));
    }

    public List<Account> getSortedAccounts(List<Account> accounts) {
        return accounts.stream()
                .sorted(Comparator.comparing(Account::getLastName).thenComparing(Account::getFirstName))
                .collect(Collectors.toList());
    }

    public Account getOldestAccount(List<Account> accounts) {
        return accounts.stream()
                .min(Comparator.comparing(Account::getBirthday))
                .orElse(null);
    }

    public Map<Integer, Double> groupByBirthYearAndAverageBalance(List<Account> accounts) {
        return accounts.stream()
                .collect(Collectors.groupingBy(
                        account -> account.getBirthday().getYear(),
                        Collectors.averagingDouble(Account::getBalance)));
    }

    public Account getAccountWithLongestLastName(List<Account> accounts) {
        return accounts.stream()
                .max(Comparator.comparing(account -> account.getLastName().length()))
                .orElse(null);
    }
}