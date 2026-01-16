package jm;

import lombok.*;

import javax.persistence.*;
import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Entity
@Table(name = "purchase_info")
public class PurchaseInfo {

    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "purchase_time_stamp")
    private LocalDateTime purchaseTimeStamp;

    @ManyToOne(targetEntity = Item.class, fetch = FetchType.EAGER)
    @JoinTable(
            name = "purchase_info_item",
            joinColumns = @JoinColumn(
                    name = "purchase_info_id",
                    referencedColumnName = "id"
            ),
            inverseJoinColumns = @JoinColumn(
                    name = "item_id",
                    referencedColumnName = "id"
            )
    )
    private Item item;

    @Column(name = "purchase_price")
    private Double purchasePrice;

    public PurchaseInfo(Item item) {
        this.purchaseTimeStamp = LocalDateTime.now();
        this.item = item;
        this.purchasePrice = item.getPrice();
    }

}