package study.realworld.entity;


import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;

import static com.google.common.base.Preconditions.checkArgument;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class User extends BaseEntity {

    private String name;

    private int money;


    public User(String name, int money) {
        checkArgument(money > 0, "사용자의 소지금은 0원 이하일 수 없습니다.");
        this.name = name;
        this.money = money;
    }
}