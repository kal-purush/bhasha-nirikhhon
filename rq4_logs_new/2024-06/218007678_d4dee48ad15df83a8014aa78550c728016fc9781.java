package org.openmrs.module.eptsreports.reporting.library.dimensions;

import java.util.Date;
import org.openmrs.Location;
import org.openmrs.module.eptsreports.reporting.library.cohorts.TB7AdvancedDiseaseAndTBCohortQueries;
import org.openmrs.module.eptsreports.reporting.utils.EptsReportUtils;
import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.indicator.dimension.CohortDefinitionDimension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TB7CascadeReportDimensions {

  @Autowired private TB7AdvancedDiseaseAndTBCohortQueries tb7CohortQueries;

  public CohortDefinitionDimension getCascadeOneIndicatorOneDimension() {
    final CohortDefinitionDimension dim = new CohortDefinitionDimension();
    dim.setName("cascadeOneIndicatorOneDimension");

    dim.addParameter(new Parameter("startDate", "Start Date", Date.class));
    dim.addParameter(new Parameter("endDate", "End Date", Date.class));
    dim.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${endDate-2m+1d},endDate=${endDate-1m},location=${location}";

    dim.addCohortDefinition(
        "artInitiation",
        EptsReportUtils.map(
            this.tb7CohortQueries.findPatientsWhoAreNewlyEnrolledOnArt(), mappings));

    dim.addCohortDefinition(
        "pregnant", Mapped.mapStraightThrough(this.tb7CohortQueries.getPatientsWhoArePregnant()));

    dim.addCohortDefinition(
        "consecutiveHighVL",
        EptsReportUtils.map(
            this.tb7CohortQueries.findPatientsWithConsecutiveViralLoadResults(), mappings));

    dim.addCohortDefinition(
        "artRestart",
        EptsReportUtils.map(
            this.tb7CohortQueries.findPatientsWhoReinitiatedARTTreatment(), mappings));

    return dim;
  }

  public CohortDefinitionDimension getCascadeOneIndicatorTwoDimension() {
    final CohortDefinitionDimension dim = new CohortDefinitionDimension();
    dim.setName("cascadeOneIndicatorTwoDimension");

    dim.addParameter(new Parameter("startDate", "Start Date", Date.class));
    dim.addParameter(new Parameter("endDate", "End Date", Date.class));
    dim.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${endDate-2m+1d},endDate=${endDate-1m},location=${location}";

    dim.addCohortDefinition(
        "artInitiation",
        EptsReportUtils.map(
            this.tb7CohortQueries.findPatientsWhoAreNewlyEnrolledOnArtWithEligibleCD4Within33Days(),
            mappings));

    dim.addCohortDefinition(
        "pregnant",
        Mapped.mapStraightThrough(this.tb7CohortQueries.getPatientsWhoArePregnantsWithCountCD4()));

    dim.addCohortDefinition(
        "consecutiveHighVL",
        EptsReportUtils.map(
            this.tb7CohortQueries.findPatientsWithConsecutiveViralLoadResultsWithCD4Count(),
            mappings));

    dim.addCohortDefinition(
        "artRestart",
        EptsReportUtils.map(
            this.tb7CohortQueries.findPatientsWhoReinitiatedARTTreatmentAndHaveCD4Count(),
            mappings));

    return dim;
  }
}