package market.domain;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.HashSet;
import java.util.Set;

/**
 * категория товаров
 */
@Entity
public class Category {

    /**
     * список продуктов в данной категории
     */
    @ManyToMany(mappedBy = "categories")
    private Set<Product> products = new HashSet<>();

    @Id
    @GeneratedValue
    private Long id;
    @Version
    private Integer version;
    /**
     * название категории. Максимум 255 символов. Обязательное поле
     */
    @Size(max = 255)
    @NotNull
    private String name;
    /**
     * описание категории, не обязательное поле
     */
    @Size(max = 500)
    private String description;

    /**
     * для JPA
     */
    public Category() {
        name = null;
        description = null;
    }

    public Category(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public Long getId() {
        return id;
    }

    public Integer getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}