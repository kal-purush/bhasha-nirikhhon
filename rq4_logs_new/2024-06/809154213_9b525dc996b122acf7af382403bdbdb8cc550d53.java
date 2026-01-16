package de.ait_tr.g_40_shop.domain.dto;

import java.math.BigDecimal;
import java.util.Objects;

public class ProductDto {

    private Long id;
    private String title;
    private BigDecimal price;

    public Long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public BigDecimal getPrice() {
        return price;
    }


    public void setId(Long id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProductDto that)) return false;
        return Objects.equals(getId(), that.getId()) && Objects.equals(getTitle(), that.getTitle()) && Objects.equals(getPrice(), that.getPrice());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getTitle(), getPrice());
    }

    @Override
    public String toString() {
        return String.format("Product DTO: id - %d, title - %s, price - %s",
                id, title, price);
    }
}