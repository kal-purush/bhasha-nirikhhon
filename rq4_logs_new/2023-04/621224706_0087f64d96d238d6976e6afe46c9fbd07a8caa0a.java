package JPABook.JPAShop.Domain;

import JPABook.JPAShop.Domain.Item.Item;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Getter
@Setter
public class Category {
    @Id
    @GeneratedValue
    @Column(name = "category_id")
    private Long id;

    private String name;

    @ManyToMany //ManyToMany 의 예제
    @JoinTable(name = "category_item", //다대다가 테이블 상에서는 불가능 하기 때문에 풀어주는 테이블이 필요하다.
            joinColumns = @JoinColumn(name = "category_id"),
            inverseJoinColumns = @JoinColumn(name = "item_id")
    )
    private List<Item> items = new ArrayList<>();

    // 카테고리 구조를 설계할 때 (계층 구조) 부모, 지삭 등의 계열을 알아야 할 때 설계 방법
    @ManyToOne
    @JoinColumn(name = "parent_id")
    private Category parent;

    @OneToMany(mappedBy = "parent")
    private List<Category> child = new ArrayList<>();
}