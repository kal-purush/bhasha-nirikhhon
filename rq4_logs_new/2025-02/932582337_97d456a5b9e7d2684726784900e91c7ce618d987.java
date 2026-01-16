package com.j30n.stoblyx.adapter.out.persistence.entity;

import com.j30n.stoblyx.domain.base.BaseTimeEntity;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Comment;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 책 정보를 저장하는 JPA 엔티티
 * books 테이블과 매핑됩니다.
 */
@Entity
@Table(
    name = "books",
    indexes = {
        @Index(name = "idx_book_title", columnList = "title"),
        @Index(name = "idx_book_author", columnList = "author"),
        @Index(name = "idx_book_genre", columnList = "genre")
    }
)
@Getter
@Setter
@NoArgsConstructor
public class BookJpaEntity extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Comment("책 고유 식별자")
    private Long id;

    @NotBlank
    @Size(max = 255)
    @Column(nullable = false)
    @Comment("책 제목")
    private String title;

    @NotBlank
    @Size(max = 255)
    @Column(nullable = false)
    @Comment("저자")
    private String author;

    @Size(max = 100)
    @Column
    @Comment("책 장르")
    private String genre;

    @NotNull
    @PastOrPresent
    @Column(nullable = false)
    @Comment("출판일")
    private LocalDate publishedAt;

    @OneToMany(mappedBy = "book", cascade = CascadeType.ALL, orphanRemoval = true)
    @Comment("책에 포함된 인용구 목록")
    private List<QuoteJpaEntity> quotes = new ArrayList<>();

    @OneToOne(mappedBy = "book", cascade = CascadeType.ALL, orphanRemoval = true)
    @Comment("책 요약")
    private SummaryJpaEntity summary;

    /**
     * 인용구를 추가합니다.
     * 양방향 관계를 유지하기 위해 인용구의 book 필드도 설정합니다.
     */
    public void addQuote(QuoteJpaEntity quote) {
        quotes.add(quote);
        quote.setBook(this);
    }

    /**
     * 인용구를 제거합니다.
     * 양방향 관계를 유지하기 위해 인용구의 book 필드를 null로 설정합니다.
     */
    public void removeQuote(QuoteJpaEntity quote) {
        quotes.remove(quote);
        quote.setBook(null);
    }

    /**
     * 요약을 설정합니다.
     * 양방향 관계를 유지하기 위해 요약의 book 필드도 설정합니다.
     */
    public void setSummary(SummaryJpaEntity summary) {
        this.summary = summary;
        if (summary != null) {
            summary.setBook(this);
        }
    }
} 