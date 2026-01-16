package pl.khamul.handworkshop;


import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import pl.khamul.handworkshop.entity.Product;
import pl.khamul.handworkshop.entity.ReservationItem;
import pl.khamul.handworkshop.repository.ProductRepository;
import pl.khamul.handworkshop.repository.ReservationRepo;

import java.util.List;

@Component
public class ScheduledTasks {

    private final ReservationRepo reservationRepo;
    private final ProductRepository productRepository;

    public ScheduledTasks(ReservationRepo reservationRepo, ProductRepository productRepository) {
        this.reservationRepo = reservationRepo;
        this.productRepository = productRepository;
    }

    @Scheduled(fixedDelay = 3000)
    public void scheduleFixedDelayTask() {
        List<ReservationItem> list = reservationRepo.findAll();

        for (ReservationItem item : list) {
            Long id = item.getProduct().getId();
            Product product = productRepository.getOne(id);
            product.setStoragequantity(product.getStoragequantity() + item.getReservedQuantity());
            item.setReservedQuantity(0);

            productRepository.save(product);
        }



    }
    @Scheduled(cron = "30 20 * * * ?")
    public void clearReservation() {
        List<ReservationItem> list = reservationRepo.findAll();

        for (ReservationItem item : list) {
            Long id = item.getProduct().getId();
            Product product = productRepository.getOne(id);
            product.setStoragequantity(product.getStoragequantity() + item.getReservedQuantity());
            item.setReservedQuantity(0);

            productRepository.save(product);
        }
    }


}