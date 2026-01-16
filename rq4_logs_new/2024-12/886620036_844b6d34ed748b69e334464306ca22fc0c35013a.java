package JAVA6.Model;
import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "ProductColor")
public class ProductColorModel {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @ManyToOne
    @JoinColumn(name = "product_id", referencedColumnName = "id", nullable = false)
    private ProductModel product;

    @ManyToOne
    @JoinColumn(name = "color_id", referencedColumnName = "id", nullable = false)
    private ColorModel color;
}