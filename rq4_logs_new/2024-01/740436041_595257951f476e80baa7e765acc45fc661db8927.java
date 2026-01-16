package com.studyproject.boardproject.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@ToString
@Table(indexes = {
    @Index(columnList = "userId"),
    @Index(columnList = "email", unique = true),
    @Index(columnList = "createAt"),
    @Index(columnList = "createBy")
})
@Entity
public class UserAccount extends AuditingFields{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Setter @Column(nullable = false, length = 50) private String userId;
    @Setter @Column(nullable = false) private String userPassword;

    @Setter @Column(length = 100) private String email;
    @Setter @Column(length = 100) private String nickName;
    @Setter private String memo;

    protected UserAccount() {}

    public UserAccount(String userId, String userPassword, String email, String nickName, String memo) {
        this.userId = userId;
        this.userPassword = userPassword;
        this.email = email;
        this.nickName = nickName;
        this.memo = memo;
    }

    public static UserAccount of(String userId, String userPassword, String email, String nickName, String memo) {
        return new UserAccount(userId, userPassword, email, nickName, memo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserAccount that)) return false;
        return id != null && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}