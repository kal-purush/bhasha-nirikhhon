package pl.khamul.handworkshop.service;

import org.springframework.stereotype.Service;
import pl.khamul.handworkshop.entity.Product;
import pl.khamul.handworkshop.entity.ReservationItem;
import pl.khamul.handworkshop.repository.ProductRepository;
import pl.khamul.handworkshop.repository.ReservationRepo;

@Service
public class ProductService implements ProductServiceInterface  {

    private final ProductRepository productRepository;
    private final ReservationRepo reservationRepo;

    public ProductService(ProductRepository productRepository, ReservationRepo reservationRepo) {
        this.productRepository = productRepository;
        this.reservationRepo = reservationRepo;
    }

    public void saveProduct(Product product){
        productRepository.save(product);
        ReservationItem reservationItem=new ReservationItem(product, 0L);
        reservationRepo.save(reservationItem);
    }
}