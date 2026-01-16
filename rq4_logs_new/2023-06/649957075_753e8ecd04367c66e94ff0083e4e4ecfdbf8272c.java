package ar.com.lokywolf2295.proyectocarritodecompras.model.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import jakarta.persistence.*;
import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.Where;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "products")
@ApiModel("Clase Producto: entidad que representar un articulo a comprar e ingresar en el Chango")
@SQLDelete(sql = "UPDATE products SET enable = false WHERE id = ?")
@Where(clause = "enable = true")
public class Product {

    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @ApiModelProperty("Clave primaria autoincremental tipo Long")
    private Long id;

    @Column(name = "name", nullable = false, length = 50)
    @ApiModelProperty("Nombre del Articulo")
    private String name;

    @Column(name = "category", nullable = false, length = 30)
    private String category;

    @Column(name = "subcategory", length = 30)
    private String subcategory;

    @Column(name = "description", length = 50)
    private String description;

    @ApiModelProperty("Precio en pesos argentinos, con 2 decimales, utilizando el . punto como separador")
    @Column(name = "price", nullable = false, scale = 2)
    private double price;

    @ApiModelProperty("Cantidad de productos, numero entero o con 2 decimales, utilizando el . punto como separador")
    @Column(name = "quantity", nullable = false, scale = 2)
    private double quantity;

    @Lob
    @Column(name = "photo", columnDefinition = "LONGTEXT")
    private String photo;

    @Column(name = "enable")
    @ApiModelProperty("Permite corroborar si el producto esta habilitado o no")
    private boolean enabled = true;

    @ManyToOne
    @JoinColumn(name = "id_cart")
    @JsonIgnore
    private ShoppingCart shoppingCartProductos;
}