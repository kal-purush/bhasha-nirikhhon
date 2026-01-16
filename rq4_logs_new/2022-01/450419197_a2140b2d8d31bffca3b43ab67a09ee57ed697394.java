package com.example.productService.Component;

import com.example.productService.dto.MerchantProductDto;
import com.example.productService.dto.OrderProductDto;
import com.example.productService.entity.Product;
import com.example.productService.service.ProductService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RabitmqComponent {

    @Autowired
    ProductService productService;

    @RabbitListener(queues = "queue.OrderProduct")
    public void updateQuantity(OrderProductDto orderProductDto){


        Product product = productService.get(orderProductDto.getProductId());
        System.out.println(product.getId()+product.getMerchantProducts());
        List<MerchantProductDto> merchantProductDtos = product.getMerchantProducts();

        for(MerchantProductDto merchantProductDto : merchantProductDtos){
            System.out.println(merchantProductDto.getMerchantId() + "outer" + orderProductDto.getMerchantId());
            if(merchantProductDto.getMerchantId().equals(orderProductDto.getMerchantId())){
                merchantProductDto.setStock(merchantProductDto.getStock()-orderProductDto.getQuantity());
                break;
            }
        }

        product.setMerchantProducts(merchantProductDtos);
        productService.add(product);

    }
}

