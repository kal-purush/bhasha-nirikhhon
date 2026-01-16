package org.openmrs.module.eptsreports.reporting.library.cohorts;

import java.util.Date;
import org.openmrs.Location;
import org.openmrs.module.eptsreports.reporting.library.queries.ListOfPatientsElegibleToCD4RequestQueries;
import org.openmrs.module.eptsreports.reporting.utils.EptsQuerysUtils;
import org.openmrs.module.eptsreports.reporting.utils.EptsReportUtils;
import org.openmrs.module.reporting.cohort.definition.CohortDefinition;
import org.openmrs.module.reporting.cohort.definition.CompositionCohortDefinition;
import org.openmrs.module.reporting.cohort.definition.SqlCohortDefinition;
import org.openmrs.module.reporting.definition.library.DocumentedDefinition;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.springframework.stereotype.Component;

@Component
public class ListOfPatientsElegibleToCD4RequestCohortQueries {
  private static final String TRASFERED_OUT_AND_DEAD = "ELEGIBLECD4/TRASFERED_OUT_AND_DEAD.sql";

  @DocumentedDefinition(value = "C1")
  public CohortDefinition C1() {
    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("C1");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    definition.setQuery(ListOfPatientsElegibleToCD4RequestQueries.QUERY.C1);

    return definition;
  }

  @DocumentedDefinition(value = "C2")
  public CohortDefinition C2() {
    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("C2");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    definition.setQuery(ListOfPatientsElegibleToCD4RequestQueries.QUERY.C2);

    return definition;
  }

  @DocumentedDefinition(value = "C3")
  public CohortDefinition C3() {
    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("C3");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    definition.setQuery(ListOfPatientsElegibleToCD4RequestQueries.QUERY.C3);

    return definition;
  }

  @DocumentedDefinition(value = "C4")
  public CohortDefinition C4() {
    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("C4");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    definition.setQuery(ListOfPatientsElegibleToCD4RequestQueries.QUERY.C4);

    return definition;
  }

  @DocumentedDefinition(value = "C5")
  public CohortDefinition C5() {
    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("C5");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    definition.setQuery(ListOfPatientsElegibleToCD4RequestQueries.QUERY.C5);

    return definition;
  }

  @DocumentedDefinition(value = "C6")
  public CohortDefinition C6() {
    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("C6");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    definition.setQuery(ListOfPatientsElegibleToCD4RequestQueries.QUERY.C6);

    return definition;
  }

  @DocumentedDefinition(value = "findTransferedOutAndDead")
  public CohortDefinition findTransferedOutAndDead() {
    final SqlCohortDefinition definition = new SqlCohortDefinition();

    definition.setName("findTransferedOutAndDead");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    definition.setQuery(EptsQuerysUtils.loadQuery(TRASFERED_OUT_AND_DEAD));

    return definition;
  }

  @DocumentedDefinition(value = "getTotaC1")
  public CohortDefinition getTotaC1() {
    final CompositionCohortDefinition definition = new CompositionCohortDefinition();

    definition.setName("getTotaC1");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${startDate},endDate=${endDate},location=${location}";

    definition.addSearch("C1", EptsReportUtils.map(this.C1(), mappings));

    definition.addSearch("C2", EptsReportUtils.map(this.C2(), mappings));
    definition.addSearch("C3", EptsReportUtils.map(this.C3(), mappings));

    definition.addSearch("C4", EptsReportUtils.map(this.C4(), mappings));
    definition.addSearch("C5", EptsReportUtils.map(this.C5(), mappings));
    definition.addSearch("C6", EptsReportUtils.map(this.C6(), mappings));

    definition.addSearch(
        "TROUTDEAD", EptsReportUtils.map(this.findTransferedOutAndDead(), mappings));

    definition.setCompositionString(" C1 NOT(C2 OR C3 OR C4 OR C5 OR C6 OR TROUTDEAD)");

    return definition;
  }

  @DocumentedDefinition(value = "getTotaC2")
  public CohortDefinition getTotaC2() {
    final CompositionCohortDefinition definition = new CompositionCohortDefinition();

    definition.setName("getTotaC2");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${startDate},endDate=${endDate},location=${location}";

    definition.addSearch("C1", EptsReportUtils.map(this.C1(), mappings));

    definition.addSearch("C2", EptsReportUtils.map(this.C2(), mappings));
    definition.addSearch("C3", EptsReportUtils.map(this.C3(), mappings));

    definition.addSearch("C4", EptsReportUtils.map(this.C4(), mappings));
    definition.addSearch("C5", EptsReportUtils.map(this.C5(), mappings));
    definition.addSearch("C6", EptsReportUtils.map(this.C6(), mappings));

    definition.addSearch(
        "TROUTDEAD", EptsReportUtils.map(this.findTransferedOutAndDead(), mappings));

    definition.setCompositionString(" C2 NOT(C1 OR C3 OR C4 OR C5 OR C6 OR TROUTDEAD)");

    return definition;
  }

  @DocumentedDefinition(value = "getTotaC3")
  public CohortDefinition getTotaC3() {
    final CompositionCohortDefinition definition = new CompositionCohortDefinition();

    definition.setName("getTotaC3");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${startDate},endDate=${endDate},location=${location}";

    definition.addSearch("C1", EptsReportUtils.map(this.C1(), mappings));

    definition.addSearch("C2", EptsReportUtils.map(this.C2(), mappings));
    definition.addSearch("C3", EptsReportUtils.map(this.C3(), mappings));

    definition.addSearch("C4", EptsReportUtils.map(this.C4(), mappings));
    definition.addSearch("C5", EptsReportUtils.map(this.C5(), mappings));
    definition.addSearch("C6", EptsReportUtils.map(this.C6(), mappings));

    definition.addSearch(
        "TROUTDEAD", EptsReportUtils.map(this.findTransferedOutAndDead(), mappings));

    definition.setCompositionString(" C3 NOT(C1 OR C2 OR C4 OR C5 OR C6 OR TROUTDEAD)");

    return definition;
  }

  @DocumentedDefinition(value = "getTotaC4")
  public CohortDefinition getTotaC4() {
    final CompositionCohortDefinition definition = new CompositionCohortDefinition();

    definition.setName("getTotaC3");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${startDate},endDate=${endDate},location=${location}";

    definition.addSearch("C1", EptsReportUtils.map(this.C1(), mappings));

    definition.addSearch("C2", EptsReportUtils.map(this.C2(), mappings));
    definition.addSearch("C3", EptsReportUtils.map(this.C3(), mappings));

    definition.addSearch("C4", EptsReportUtils.map(this.C4(), mappings));
    definition.addSearch("C5", EptsReportUtils.map(this.C5(), mappings));
    definition.addSearch("C6", EptsReportUtils.map(this.C6(), mappings));

    definition.addSearch(
        "TROUTDEAD", EptsReportUtils.map(this.findTransferedOutAndDead(), mappings));

    definition.setCompositionString(" C4 NOT(C1 OR C2 OR C3 OR C5 OR C6 OR TROUTDEAD)");

    return definition;
  }

  @DocumentedDefinition(value = "getTotaC5")
  public CohortDefinition getTotaC5() {
    final CompositionCohortDefinition definition = new CompositionCohortDefinition();

    definition.setName("getTotaC3");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${startDate},endDate=${endDate},location=${location}";

    definition.addSearch("C1", EptsReportUtils.map(this.C1(), mappings));

    definition.addSearch("C2", EptsReportUtils.map(this.C2(), mappings));
    definition.addSearch("C3", EptsReportUtils.map(this.C3(), mappings));

    definition.addSearch("C4", EptsReportUtils.map(this.C4(), mappings));
    definition.addSearch("C5", EptsReportUtils.map(this.C5(), mappings));
    definition.addSearch("C6", EptsReportUtils.map(this.C6(), mappings));

    definition.addSearch(
        "TROUTDEAD", EptsReportUtils.map(this.findTransferedOutAndDead(), mappings));

    definition.setCompositionString(" C5 NOT(C1 OR C2 OR C3 OR C4 OR C6 OR TROUTDEAD)");

    return definition;
  }

  @DocumentedDefinition(value = "getTotaC6")
  public CohortDefinition getTotaC6() {
    final CompositionCohortDefinition definition = new CompositionCohortDefinition();

    definition.setName("getTotaC3");
    definition.addParameter(new Parameter("startDate", "Start Date", Date.class));
    definition.addParameter(new Parameter("endDate", "End Date", Date.class));
    definition.addParameter(new Parameter("location", "location", Location.class));

    final String mappings = "startDate=${startDate},endDate=${endDate},location=${location}";

    definition.addSearch("C1", EptsReportUtils.map(this.C1(), mappings));

    definition.addSearch("C2", EptsReportUtils.map(this.C2(), mappings));
    definition.addSearch("C3", EptsReportUtils.map(this.C3(), mappings));

    definition.addSearch("C4", EptsReportUtils.map(this.C4(), mappings));
    definition.addSearch("C5", EptsReportUtils.map(this.C5(), mappings));
    definition.addSearch("C6", EptsReportUtils.map(this.C6(), mappings));

    definition.addSearch(
        "TROUTDEAD", EptsReportUtils.map(this.findTransferedOutAndDead(), mappings));

    definition.setCompositionString(" C6 NOT(C1 OR C2 OR C3 OR C4 OR C5 OR TROUTDEAD)");

    return definition;
  }
}