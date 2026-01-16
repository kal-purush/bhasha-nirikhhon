package ar.com.lokywolf2295.proyectocarritodecompras.model.mapper;

import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.request.ProductRequestDTO;
import ar.com.lokywolf2295.proyectocarritodecompras.model.dto.response.ProductResponseDTO;
import ar.com.lokywolf2295.proyectocarritodecompras.model.entity.Product;
import org.springframework.stereotype.Component;

import java.util.function.Function;

import static java.lang.Boolean.FALSE;

@Component
public class ProductMapper implements Function<Product, ProductResponseDTO> {

    public Product toEntity(ProductRequestDTO productRequestDTO) {
        Product product = new Product();

        product.setName(productRequestDTO.getName());
        product.setCategory(productRequestDTO.getCategory());
        product.setDescription(productRequestDTO.getDescription());
        product.setPrice(productRequestDTO.getPrice());
        product.setQuantity(productRequestDTO.getQuantity());
        product.setPhoto(productRequestDTO.getPhoto());

        return product;
    }

    @Override
    public ProductResponseDTO apply(Product product) {
        ProductResponseDTO productResponseDTO = new ProductResponseDTO();

        productResponseDTO.setId(product.getId());
        productResponseDTO.setName(product.getName());
        productResponseDTO.setPhoto(product.getPhoto());
        productResponseDTO.setCategory(product.getCategory());
        productResponseDTO.setDescription(product.getDescription());
        productResponseDTO.setPrice(product.getPrice());
        productResponseDTO.setQuantity(product.getQuantity());
        productResponseDTO.setEnabled(product.isEnabled());

        return productResponseDTO;
    }

    public Product refreshValues(Product product, ProductRequestDTO productRequestDTO) {
        if (productRequestDTO.getName() != null && !productRequestDTO.getName().trim().isEmpty())
            product.setName(productRequestDTO.getName());
        if (productRequestDTO.getDescription() != null && !productRequestDTO.getDescription().trim().isEmpty())
            product.setDescription(productRequestDTO.getDescription());
        if (productRequestDTO.getPrice() != 0 && productRequestDTO.getPrice() >= 0)
            product.setPrice(productRequestDTO.getPrice());
        if (productRequestDTO.getQuantity() != 0 && productRequestDTO.getQuantity() >= 0)
            product.setQuantity(productRequestDTO.getQuantity());
        if (!productRequestDTO.isEnabled())
            product.setEnabled(FALSE);

        return product;
    }
}