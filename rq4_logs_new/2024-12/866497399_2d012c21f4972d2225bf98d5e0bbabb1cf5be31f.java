package com.rodrigomoreira.msstock.services;

import com.rodrigomoreira.msstock.model.Product;
import com.rodrigomoreira.msstock.repositories.ProductRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductService {

    @Autowired
    private ProductRepository productRepository;

    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    public Product getProductByName(String name){
        return productRepository.findByName(name.toLowerCase());
    }

    public Product addProduct(Product product){
        product.setName(product.getName().toLowerCase());
        Product storedProduct = productRepository.findByName(product.getName());
        if (storedProduct != null){
            storedProduct.setQuantity(storedProduct.getQuantity() + product.getQuantity());
            return productRepository.save(storedProduct);
        }

        return productRepository.save(product);
    }

    public Product updateProductQuantity(String name, int quantity){
        Product product = productRepository.findByName(name.toLowerCase());
        if (product != null){
            product.setQuantity(quantity);
            return productRepository.save(product);
        }

        throw new RuntimeException("Product not found");
    }

    public void deleteProductById(Long id){
        productRepository.deleteById(id);
    }

    @KafkaListener(topics = "estoque-topico", groupId = "estoque-group")
    public void isProductAvailable(Product product){
        Product storedProduct = productRepository.findByName(product.getName());
        if (storedProduct != null && product.getQuantity() <= storedProduct.getQuantity()){
            System.out.println("Venda de: " + product.getQuantity() + " " + product.getName() + " realizada com sucesso.");
            storedProduct.setQuantity(storedProduct.getQuantity() - product.getQuantity());
            productRepository.save(storedProduct);
        } else {
            System.out.println("Venda não realizada. Estoque insuficiente");
        }
    }
}