package org.ctc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.ctc.entity.Image;
import org.ctc.entity.OrderDetail;
import org.ctc.entity.ProdSpec;
import org.ctc.entity.Product;

import java.util.List;
@Data
@AllArgsConstructor
public class AddedProductDTO {
    private Product product;
    private List<ProdSpec> prodSpecs;
    private List<Image> imageList;
    private List<OrderDetail> orderDetails;
    private List<String> imgUrlList;

    public AddedProductDTO() {
    }

    public AddedProductDTO(Product product, List<ProdSpec> prodSpecs) {
        this.product = product;
        this.prodSpecs = prodSpecs;
    }

    public AddedProductDTO(Product product, List<ProdSpec> prodSpecs, List<Image> imageList) {
        this.product = product;
        this.prodSpecs = prodSpecs;
        this.imageList = imageList;
    }



}