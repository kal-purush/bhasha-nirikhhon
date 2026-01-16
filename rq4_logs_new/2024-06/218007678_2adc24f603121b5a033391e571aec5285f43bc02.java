package org.openmrs.module.eptsreports.reporting.library.datasets;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.openmrs.Location;
import org.openmrs.module.eptsreports.reporting.library.indicators.EptsGeneralIndicator;
import org.openmrs.module.eptsreports.reporting.utils.EptsQuerysUtils;
import org.openmrs.module.eptsreports.reporting.utils.EptsReportUtils;
import org.openmrs.module.reporting.ReportingConstants;
import org.openmrs.module.reporting.cohort.definition.CohortDefinition;
import org.openmrs.module.reporting.cohort.definition.SqlCohortDefinition;
import org.openmrs.module.reporting.dataset.definition.CohortIndicatorDataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.SqlDataSetDefinition;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ListOfPatientsChildreenEnrolledInCCRDataSet extends BaseDataSet {

  private static final String CCR_LIST = "LIST_CCR_PATIENTS/PATIENTS_CHILDREN_IN_CCR_LIST.sql";
  private static final String CCR_TOTAL = "LIST_CCR_PATIENTS/PATIENTS_CHILDREN_IN_CCR_TOTAL.sql";

  @Autowired private EptsGeneralIndicator eptsGeneralIndicator;

  public DataSetDefinition constructDataset(List<Parameter> list) {

    SqlDataSetDefinition dsd = new SqlDataSetDefinition();
    dsd.setName("Lista de Crianças Inscritas em CCR");
    dsd.addParameters(list);
    dsd.setSqlQuery(EptsQuerysUtils.loadQuery(CCR_LIST));

    return dsd;
  }

  public DataSetDefinition getTotaLOfChildrenEnrolledInCCR() {

    final CohortIndicatorDataSetDefinition dataSetDefinition =
        new CohortIndicatorDataSetDefinition();

    dataSetDefinition.setParameters(this.getParameters());

    final String mappings = "startDate=${startDate},endDate=${endDate},location=${location}";

    final CohortDefinition CCR = this.findPatientsChildrenEnrolledInCCR();

    dataSetDefinition.addColumn(
        "TOTAL",
        "Lista de Crianças Inscritas em CCR",
        EptsReportUtils.map(
            this.eptsGeneralIndicator.getIndicator(
                "Lista de Crianças Inscritas em CCR", EptsReportUtils.map(CCR, mappings)),
            mappings),
        "");

    return dataSetDefinition;
  }

  public CohortDefinition findPatientsChildrenEnrolledInCCR() {

    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("Lista de Crianças Inscritas em CCR");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    String query = EptsQuerysUtils.loadQuery(CCR_TOTAL);

    definition.setQuery(query);

    return definition;
  }

  public List<Parameter> getParameters() {
    List<Parameter> parameters = new ArrayList<Parameter>();
    parameters.add(ReportingConstants.START_DATE_PARAMETER);
    parameters.add(ReportingConstants.END_DATE_PARAMETER);
    parameters.add(ReportingConstants.LOCATION_PARAMETER);
    return parameters;
  }
}