package umgc.city.team1.models.outgoing;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MapCase {
    private String codeLabel;
    private String baseCode;
    private String overlayCode;
    private String baseCodeDescription;
    private String overlayCodeDescription;
    private String baseGeneralStandardsURL;
    private String baseAdditionalStandardsURL;
    private String baseGardenStandard;
    private String baseFrontageAndFacadesStandards;
    private String overlayGeneralStandardsURL;
    private String overlayAdditionalStandardsURL;
    private String overlayGardenStandards;
    private String overlayFrontageAndFacadesStandards;
}