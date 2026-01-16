package world.podo.emergency.application;

import org.springframework.stereotype.Component;
import world.podo.emergency.domain.country.CovidFetchValue;
import world.podo.emergency.ui.web.CovidResponse;

@Component
public class CovidAssembler {

    public CovidResponse toCovidResponse(CovidFetchValue covidFetchValue) {
        if (covidFetchValue == null) {
            return null;
        }
        return new CovidResponse(
                covidFetchValue.getCreatedAt(),
                covidFetchValue.getTotalDeathToll(),
                covidFetchValue.getTotalConfirmCases(),
                covidFetchValue.getDeltaConfirmCases()
        );
    }
}