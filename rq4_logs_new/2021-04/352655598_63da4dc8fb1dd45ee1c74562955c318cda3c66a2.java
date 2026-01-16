package com.kodilla.ecommerce.domain;

import com.kodilla.ecommerce.domain.enums.StatusOrder;
import com.sun.istack.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import javax.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
@Entity
@Table(name = "orders")
public class Order {
    @NotNull
    @Id
    @GeneratedValue
    private Long id;

    @NotEmpty
    @OneToMany(
            targetEntity = OrderItem.class,
            mappedBy = "orderId",
            cascade = CascadeType.ALL,
            fetch = FetchType.LAZY
    )
    private List<OrderItem> items;

    @NotBlank
    private String number;

    @Enumerated(EnumType.STRING)
    private StatusOrder status;

    @NotNull
    @Size(min = 10, max = 100)
    private String shippingAddress;

    @NotNull
    private LocalDateTime date;

    @Column(name = "user_id")
    private Long userId;
}