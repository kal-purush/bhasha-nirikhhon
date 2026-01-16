package com.talentpath.amazin.api.summaryratingstars;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicInsert;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;


@Entity
@Data // Lombok annotation; tells it to create getters and setters
@AllArgsConstructor // lombok annotation to create constructor with all args
@NoArgsConstructor // lombok annotation to create no-args constructor
//@Accessors(chain = true)
@DynamicInsert
@Table(name = "bwl01", schema = "public")

public class SummaryStarRatings {

    @Id
    @Column(name = "ratingid")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long ratingId;

    @Column(name = "overallrating")
    @NotBlank
    private float overallRating;

    @Column(name = "totalfivestarrating")
    @NotBlank
    private int totalFiveStarRating;

    @Column(name = "totalfourstarrating")
    @NotBlank
    private int totalFourStarRating;

    @Column(name = "totalthreestarrating")
    @NotBlank
    private int totalThreeStarRating;

    @Column(name = "totaltwostarrating")
    @NotBlank
    private int totalTwoStarRating;

    @Column(name = "totalonestarrating")
    @NotBlank
    private int totalOneStarRating;
}