package eu.argalladas.shopping.services;

import eu.argalladas.shopping.domain.SalesDTO;

import java.time.LocalDateTime;
import java.util.Optional;

public interface SalesService {

    Optional<SalesDTO> findSale(LocalDateTime date, Long productId, Long brandId);
}