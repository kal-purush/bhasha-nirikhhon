package com.roomfit.be.survey.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity(name = "options")
@Getter
@NoArgsConstructor
public class Option {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "option_label")
    String label;
    @Column(name = "option_value")
    String value;

    @ManyToOne(fetch = FetchType.LAZY,cascade = CascadeType.PERSIST)  // Many-to-One 관계 활성화
    @JoinColumn(name = "question_id")  // question_id 컬럼을 매핑
    private Question question;

    public Option(String label, String value) {
        this.label = label;
        this.value = value;
    }
    public void setQuestion(Question question) {
        this.question = question;
    }

}