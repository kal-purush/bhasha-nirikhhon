package org.example.restaurantvoting.restaurant.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.persistence.Entity;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.example.restaurantvoting.common.model.NamedEntity;
import org.example.restaurantvoting.common.validation.View;

@Entity
@Table(name = "dish")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Dish extends NamedEntity {

    @NotNull
    @PositiveOrZero
    private long price;

    @Schema(hidden = true)
    @JsonBackReference
    @ManyToOne
    @JoinColumn(name = "restaurant_id")
    @NotNull(groups = View.Persist.class)
    private Restaurant restaurant;

    public Dish(int id, String name, long price, Restaurant restaurant) {
        super(id, name);
        this.price = price;
        this.restaurant = restaurant;
    }
}