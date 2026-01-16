package com.university.contractors.model;

import com.google.common.base.Objects;

import javax.persistence.*;

@Entity(name = "educ_languages")
public class EducationLanguage implements IdEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_educ_language")
    private Long id;
    private String educLanguageName;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEducLanguageName() {
        return educLanguageName;
    }

    public void setEducLanguageName(String educLanguageName) {
        this.educLanguageName = educLanguageName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EducationLanguage that = (EducationLanguage) o;
        return Objects.equal(id, that.id) &&
                Objects.equal(educLanguageName, that.educLanguageName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, educLanguageName);
    }
}