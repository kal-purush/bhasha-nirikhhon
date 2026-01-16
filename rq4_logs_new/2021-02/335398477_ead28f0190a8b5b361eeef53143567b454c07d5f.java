package pl.khamul.handworkshop.entity;




import javax.persistence.*;


@Entity
public class ReservationItem {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long Id;

    @OneToOne
    private Product product;

    private Long reservedQuantity;

    public ReservationItem(Product product, Long reservedQuantity) {
        this.product = product;
        this.reservedQuantity = reservedQuantity;
    }

    public ReservationItem() {
    }

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    public Long getReservedQuantity() {
        return reservedQuantity;
    }

    public void setReservedQuantity(Long reservedQuantity) {
        this.reservedQuantity = reservedQuantity;
    }
}