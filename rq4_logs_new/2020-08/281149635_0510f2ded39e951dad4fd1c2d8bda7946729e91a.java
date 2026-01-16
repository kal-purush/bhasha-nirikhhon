package com.talentpath.amazin.api.answers;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name= "answers", schema = "public")
public class Answers {

    @Id
    @Column(name = "answerid")
    private Long id;
    @Column(name = "answer")
    private String answer;
    @Column(name = "questionid")
    private Long questionid;
    // private int votes;

    public Answers(){

    }

    public Answers(long id, String answer, Long questionid){
        super();
        this.id = id;
        this.answer = answer;
        this.questionid = questionid;
        // this.votes = votes;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getanswer() {
        return answer;
    }

    public void setanswer( String answer) {
        this.answer = answer;
    }

    public Long getquestionid() {
        return questionid;
    }

    public void setquestionid(Long questionid) {
        this.questionid = questionid;
    }

    // public int getVotes() {
    //     return votes;
    // }

    // public void setVotes(int votes) {
    //     this.votes = votes;
    // }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Answers other = (Answers) obj;
        // id comparison
        if (id == null) {
            if (other.id != null) return false;
        }
        else if (!id.equals(other.id)) return false;
        // arrayOfOtherIds comparison
        if (answer == null) {
            if (other.answer != null) return false;
        }
        else if (!answer.equals(other.answer)) return false;
        return true;
    }



}