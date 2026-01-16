package bjorn.springframework.msccbrewery.services;

import bjorn.springframework.msccbrewery.web.model.BeerDto;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class BeerServiceImpl implements BeerService {
    @Override
    public BeerDto getBeerById(UUID beerId) {
        return BeerDto.builder()
                .beerName("Test Name")
                .beerStyle("Amber")
                .id(beerId)
                .upc(-1l)
                .build();
    }
}