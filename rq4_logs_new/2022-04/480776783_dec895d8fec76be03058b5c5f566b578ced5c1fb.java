package study.realworld.entity;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;

import static com.google.common.base.Preconditions.checkArgument;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Japanki extends BaseEntity {

    private String name;

    private int price;

    private int stock;


    public Japanki(String name, int price, int stock) {
        checkArgument(Long.min(price, stock) > 0, "음수로 넣을 수 없습니다.");
        this.name = name;
        this.price = price;
        this.stock = stock;
    }
}