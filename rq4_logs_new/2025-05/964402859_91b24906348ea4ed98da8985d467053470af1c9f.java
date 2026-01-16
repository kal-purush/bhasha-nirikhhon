package com.fatepet.petrest.addtionalimage;

import com.fatepet.petrest.addtionalinfo.AdditionalInfo;
import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
public class AdditionalImage {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "additional_info_id")
    private AdditionalInfo additionalInfo;

    private String imageUrl;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;


}