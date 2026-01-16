package com.eng.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED) // builder
public class Quiz {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @OneToOne(fetch = FetchType.LAZY)
    @JsonIgnore
    @JoinColumn(name="study_id")
    private Study study;

    @Column
    private boolean correct;

    @Builder
    public Quiz(Study study) {
        this.study = study;
        this.correct = false;
    }

    public static Quiz addQuiz(Study study) {
        return Quiz.builder()
                .study(study)
                .build();
    }
}