package umgc.city.team1.models.incoming;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UseCaseDto {

    private UUID zoneId;
    private String zoneSymbol;
    private String zoneDescription;
    private String applicationURL;
    private String procedureURL;
    private String permitName;
    private String permitDescription;
    private UUID zoneLandUseId;
    private String zoneLandUseDescription;
    private String generalStandardURL;
    private String gardenStandardURL;
    private String additionalStandardURL;
    private String frontageAndFacadesStandardURL;


}