package it.project1.inventory;

import it.project1.item.Item;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Check;

@Entity
@Table
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Inventory {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;
    @ManyToOne
    private Character character;
    @ManyToOne
    private Item item;
    @Column(nullable = false)
    @Check(constraints = "slot > 0")
    private int slot;
    @Column
    private int quantity;
    @Column(nullable = false)
    private  boolean isEquipped;

}