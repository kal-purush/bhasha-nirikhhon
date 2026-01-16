package com.example.springbootdeveloper;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PROTECTED) // 기본 생성자
@AllArgsConstructor
@Getter
@Entity // 엔티티로 지정
public class Member {
    @Id // id 필드를 기본키로 지정
    @GeneratedValue(strategy = GenerationType.IDENTITY) // 기본키를 자동으로 1씩 증가
    @Column(name = "id", updatable = false)
    private Long id; // DB 테이블의 'id' 컬럼과 매칭

    @Column(name = "name", updatable = false) // not null
    private String name; // DB 테이블의 'name' 컬럼과 매칭

    public void changeName(String name) {
        this.name = name;
    }
}

/*
영속성 컨텍스트 예제
public class EntityManagerTest {
    @Autowired EntityManager em;

    public void example() {
        // 비영속 상태(new) - 엔티티 메니저가 엔티티를 관리하지 않는 상태
        Member member = new Member(1L, "홍길동");

        // 영속 상태(managed) - 엔티티가 관리되는 상태
        em.persist(member);

        // 준영속 상태(detached) - 엔티티 객체가 분리된 상태
        em.detach(member);

        // 삭제(removed) - 엔티티 객체가 삭제된 상태
        em.remove(member);
    }
}
*/