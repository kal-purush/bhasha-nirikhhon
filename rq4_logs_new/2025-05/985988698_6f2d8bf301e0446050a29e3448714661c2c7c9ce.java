package application.usecase;

import domain.models.BlatSearchOptions;
import domain.port.BlatProviderPort;
import infrastructure.dtos.PromoterRegionDTO;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

@ApplicationScoped
public class GetBlatSearchOptions {

    private final BlatProviderPort blatProviderPort;

    public GetBlatSearchOptions(BlatProviderPort transcriptionFactorJasparProviderPort) {
        this.blatProviderPort = transcriptionFactorJasparProviderPort;
    }

    public List<BlatSearchOptions> getBlatSearchOptions(PromoterRegionDTO tfRequest) {
        return blatProviderPort.fetchDataFromBlatSource(tfRequest);
    }
}