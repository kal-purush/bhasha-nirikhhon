package pl.dchruscinski.event;

import jakarta.persistence.*;

import java.time.LocalDateTime;
import java.time.ZoneId;

@Entity
@Table(name = "product_availability_event")
public class PersistedProductEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;
    private int productId;
    private String name;
    private LocalDateTime updateMoment;

    public PersistedProductEvent() {
    }

    PersistedProductEvent(ProductAvailabilityEvent source) {
        productId = source.getProductId();
        name = source.getName();
        updateMoment = LocalDateTime.ofInstant(source.getUpdateMoment(), ZoneId.systemDefault());
    }
}