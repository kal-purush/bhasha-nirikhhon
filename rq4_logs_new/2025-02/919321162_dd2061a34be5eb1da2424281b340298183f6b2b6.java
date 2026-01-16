package com.fptgang.backend.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "blind_box_campaign")
@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class BlindBoxCampaign {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long historyId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "blind_box_id", nullable = false)
    private BlindBox blindBox;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "promotional_campaign_id", nullable = false)
    private PromotionalCampaign promotionalCampaign;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    @Column(nullable = false, columnDefinition = "BOOLEAN DEFAULT TRUE")
    private boolean isVisible = true;
}