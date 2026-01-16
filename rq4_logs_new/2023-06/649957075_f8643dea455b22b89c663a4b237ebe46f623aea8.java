package ar.com.lokywolf2295.proyectocarritodecompras.model.mapper;

import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.request.ShopingRequestDTO;
import ar.com.lokywolf2295.proyectocarritodecompras.model.entity.Product;
import ar.com.lokywolf2295.proyectocarritodecompras.model.entity.ShoppingCart;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ShoppingCartMapper {

    public ShoppingCart toEntity (ShopingRequestDTO dto, Product product, ShoppingCart cart){
        ShoppingCart item = new ShoppingCart();
        item.setProductsList(List.of(product));
        item.setEnabled(dto.isEnabled());
        return item;
    }
}