package com.tour.prevel.tour.domain;


import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.ColumnDefault;

@Table(name = "TB_TOUR")
@Entity
@Builder
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
public class Tour {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String contentId;
    private String contentTypeId;

    private String addr1;
    private String addr2;

    private double mapX;
    private double mapY;

    private String title;
    private String tel;

    @Setter
    private String homepage;
    @Setter
    @Column(columnDefinition = "TEXT")
    private String description;
    @Column(columnDefinition = "TEXT")
    private String thumbnail;

    @Setter
    private String playtime;

    @Setter
    private String hashTags;

    @Setter
    @Column(columnDefinition = "varchar(255) default '기타'")
    private String category;
}