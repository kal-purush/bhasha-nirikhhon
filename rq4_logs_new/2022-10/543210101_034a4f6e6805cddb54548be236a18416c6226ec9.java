package com.job_search;

import com.job_search.repository.entity.Company;
import com.job_search.repository.entity.Vacancy;

import java.util.Random;

public class DataProvider {
    private DataProvider() {
    }

    public static Company getCompany() {
        return Company.builder()
                .name("Google" + new Random().nextInt(999999))
                .description("Google company")
                .founded(1990)
                .isWorkFromHome((Math.random() < 0.5))
                .website("google.com")
                .build();
    }

    public static Vacancy getVacancy() {
        return Vacancy.builder()
                .title("QA"+ new Random().nextInt(999999))
                .description("Automation QA")
                .isOpen((Math.random() < 0.5))
                .build();
    }
}