package com.fptgang.backend.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "shipping_info")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippingInfo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long shippingInfoId;

    @Column(columnDefinition = "NVARCHAR(255) DEFAULT 'N/A'")
    private String address;

    @Column(columnDefinition = "NVARCHAR(255)", nullable = false)
    private String ward;

    @Column(columnDefinition = "NVARCHAR(255)", nullable = false)
    private String district;

    @Column(columnDefinition = "NVARCHAR(255)", nullable = false)
    private String city;

    @Column(columnDefinition = "NVARCHAR(255)", nullable = false)
    private String name;

    @Column(columnDefinition = "NVARCHAR(255)", nullable = false)
    private String phoneNumber;

    @OneToMany(mappedBy = "shippingInfo", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Order> orders;

    @Column(nullable = false, columnDefinition = "BOOLEAN DEFAULT TRUE")
    private boolean isVisible = true;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

}