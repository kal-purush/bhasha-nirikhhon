package refooding.api.domain.exchange.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import refooding.api.domain.exchange.dto.request.ExchangeCreateRequest;
import refooding.api.domain.exchange.entity.Exchange;
import refooding.api.domain.exchange.entity.Region;
import refooding.api.domain.exchange.repository.ExchangeRepository;
import refooding.api.domain.exchange.repository.RegionRepository;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ExchangeServiceImpl implements ExchangeService{

    private final ExchangeRepository exchangeRepository;
    private final RegionRepository regionRepository;

    @Transactional
    @Override
    public void create(ExchangeCreateRequest request) {
        Exchange exchange = request.toExchange();
        Region region = regionRepository.findById(request.regionId()).orElseThrow();
        exchange.updateRegion(region);

        exchangeRepository.save(exchange);
    }

}