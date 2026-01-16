package com.university.contractors.repository;

import com.google.common.collect.Lists;
import com.university.contractors.controller.payload.Nationality;
import com.university.contractors.controller.payload.SearchStudent;
import com.university.contractors.model.Contract;
import com.university.contractors.model.Country;
import com.university.contractors.service.CountryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.List;
import java.util.Objects;

@Repository
public class SearchStudentRepository {

    private final EntityManager entityManager;
    private final CountryService countryService;

    @Autowired
    public SearchStudentRepository(EntityManager entityManager, CountryService countryService) {
        this.entityManager = entityManager;
        this.countryService = countryService;
    }

    public List<SearchStudentResult> search(SearchStudent searchStudent) {
        final CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        final CriteriaQuery<SearchStudentResult> query = criteriaBuilder.createQuery(SearchStudentResult.class);
        final Root<Contract> contractRoot = query.from(Contract.class);
        final CriteriaQuery<SearchStudentResult> criteria = query.multiselect(contractRoot);

        final List<Predicate> predicateList = getListOfPredicates(searchStudent, criteriaBuilder, contractRoot, criteria);
        criteria.where(criteriaBuilder.and(predicateList.toArray(new Predicate[]{})));

        return entityManager.createQuery(criteria).getResultList();
    }

    private List<Predicate> getListOfPredicates(SearchStudent searchStudent, CriteriaBuilder criteriaBuilder, Root<Contract> contractRoot, CriteriaQuery<SearchStudentResult> criteria) {
        final List<Predicate> predicateList = Lists.newArrayList();

        if (Objects.nonNull(searchStudent.getSurname())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("student").get("surname"),
                    searchStudent.getSurname()));
        }

        if (Objects.nonNull(searchStudent.getName())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("student").get("name"),
                    searchStudent.getName()));
        }

        if (Objects.nonNull(searchStudent.getDataOfBirth())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("student").get("dateOfBirth"),
                    searchStudent.getDataOfBirth()));
        }

        if (Objects.nonNull(searchStudent.getNationality())) {
            final Nationality nationality = searchStudent.getNationality();
            final Country localCountry = countryService.getLocalCountry();
            if (nationality == Nationality.LOCAL) {
                predicateList.add(criteriaBuilder.equal(contractRoot.get("student").get("country"),
                        localCountry));
            } else {
                predicateList.add(criteriaBuilder.notEqual(contractRoot.get("student").get("country"),
                        localCountry));
            }
        }

        if (Objects.nonNull(searchStudent.getCountry())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("student").get("country"),
                    searchStudent.getCountry()));
        }

        if (Objects.nonNull(searchStudent.getContractNumber())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("contractNumber"),
                    searchStudent.getContractNumber()));
        }

        if (Objects.nonNull(searchStudent.getCourse())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("course"),
                    searchStudent.getCourse()));
        }

        if (Objects.nonNull(searchStudent.getFaculty())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("direction").get("faculty"),
                    searchStudent.getFaculty()));
        }

        if (Objects.nonNull(searchStudent.getDirection())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("direction"),
                    searchStudent.getDirection()));
        }

        if (Objects.nonNull(searchStudent.getEducationProgram())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("educationProgram"),
                    searchStudent.getEducationProgram()));
        }

        if (Objects.nonNull(searchStudent.getEducationLevel())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("educationLevel"),
                    searchStudent.getEducationLevel()));
        }

        if (Objects.nonNull(searchStudent.getEducationForm())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("educationForm"),
                    searchStudent.getEducationForm()));
        }

        if (Objects.nonNull(searchStudent.getArrivalLine())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("arrivalLine"),
                    searchStudent.getArrivalLine()));
        }

        if (Objects.nonNull(searchStudent.getEducationLanguage())) {
            predicateList.add(criteriaBuilder.equal(contractRoot.get("educationLanguage"),
                    searchStudent.getEducationLanguage()));
        }

        return predicateList;
    }

}