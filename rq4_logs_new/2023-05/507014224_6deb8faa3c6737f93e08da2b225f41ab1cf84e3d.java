package com.discphy.code.programmers.woowa;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static java.time.LocalDate.parse;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TechCamp2023Test {

    @Test
    void solution1TestCase1() {
        String today = "2022.05.19";
        String[] terms = new String[]{"A 6", "B 12", "C 3"};
        String[] privacies = {"2021.05.02 A", "2021.07.01 B", "2022.02.19 C", "2022.02.20 C"};

        int[] result = solution1(today, terms, privacies);

        assertThat(result).containsExactly(1, 3);
    }

    @Test
    void solution1TestCase2() {
        String today = "2020.01.01";
        String[] terms = new String[]{"Z 3", "D 5"};
        String[] privacies = {"2019.01.01 D", "2019.11.15 Z", "2019.08.02 D", "2019.07.01 D", "2018.12.28 Z"};

        int[] result = solution1(today, terms, privacies);

        assertThat(result).containsExactly(1, 4, 5);
    }

    //https://school.programmers.co.kr/learn/courses/30/lessons/150370
    private int[] solution1(String today, String[] terms, String[] privacies) {
        List<Integer> result = new ArrayList<>();
        LocalDate date = stringToDate(today);

        System.out.println("date = " + date);

        List<Term> termsList = stream(terms).map(Term::of).collect(toList());
        List<Privacy> privacyList = stream(privacies).map(p -> Privacy.of(p, termsList)).collect(toList());

        for (int i = 0; i < privacyList.size(); i++) {
            Privacy privacy = privacyList.get(i);

            if (privacy.isExpired(date)) {
                System.out.println("privacy = " + privacy.getCompareDate() + " / " + privacy);
                result.add(i + 1);
            }
        }

        return result.stream().mapToInt(Integer::intValue).toArray();
    }

    private LocalDate stringToDate(String today) {
        return parse(today, ofPattern("yyyy.MM.dd"));
    }

    public static class Term {

        private final String type;
        private final Integer expireMonth;

        public Term(String type, Integer expireMonth) {
            this.type = type;
            this.expireMonth = expireMonth;
        }

        public static Term of(String term) {
            validate(term);
            return new Term(term.split(" ")[0], Integer.valueOf(term.split(" ")[1]));
        }

        private static void validate(String privacy) {
            if (privacy.split(" ").length != 2) throw new IllegalArgumentException("Invalid value");
        }

        public String getType() {
            return type;
        }

        public Integer getExpireMonth() {
            return expireMonth;
        }

        public boolean find(String type) {
            return this.type.equals(type);
        }

        @Override
        public String toString() {
            return "Term{" +
                    "type='" + type + '\'' +
                    ", expireMonth=" + expireMonth +
                    '}';
        }
    }

    public static class Privacy {

        private final LocalDate date;
        private final Term term;

        public Privacy(LocalDate date, Term term) {
            this.date = date;
            this.term = term;
        }

        public static Privacy of(String privacy, List<Term> terms) {
            validate(privacy);
            LocalDate date = parse(privacy.split(" ")[0], ofPattern("yyyy.MM.dd"));
            Term term = findTerm(privacy.split(" ")[1], terms);

            return new Privacy(date, term);
        }

        private static Term findTerm(String termType, List<Term> terms) {
            return terms.stream()
                    .filter(t -> t.find(termType))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Not found term with type"));
        }

        private static void validate(String privacy) {
            if (privacy.split(" ").length != 2) throw new IllegalArgumentException("Invalid value");
        }

        public LocalDate getDate() {
            return date;
        }

        public Term getTerm() {
            return term;
        }

        public LocalDate getCompareDate() {
            LocalDate target = date.plusMonths(term.getExpireMonth());

            if (date.getDayOfMonth() == 1) {
                return target.minusMonths(1).withDayOfMonth(28);
            }

            return target.minusDays(1);
        }

        public boolean isExpired(LocalDate today) {
            return today.isAfter(getCompareDate());
        }

        @Override
        public String toString() {
            return "Privacy{" +
                    "date=" + date +
                    ", term=" + term +
                    '}';
        }
    }
}