package world.podo.travelable.infrastructure.public_api;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import world.podo.travelable.domain.country.CovidFetchService;
import world.podo.travelable.domain.country.CovidFetchValue;

import java.time.LocalDate;
import java.util.List;

@Service(CacheableCovidFetchService.BEAN_NAME)
public class CacheableCovidFetchService implements CovidFetchService {
    public static final String BEAN_NAME = "cacheableCovidFetchService";

    private final CovidFetchService covidFetchService;

    public CacheableCovidFetchService(
            @Qualifier(CovidFetchServiceImpl.BEAN_NAME) CovidFetchService covidFetchService
    ) {
        this.covidFetchService = covidFetchService;
    }

    @Cacheable("COVID")
    @Override
    public List<CovidFetchValue> fetch(LocalDate localDate) {
        return covidFetchService.fetch(localDate);
    }
}