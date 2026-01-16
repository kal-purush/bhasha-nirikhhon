package edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api;

public class UnsupportedAnalysisReport extends Report {

  private static String description = "Unsupported analysis was invoked. Refer to the documentation to do the correct analysis operation";
  public UnsupportedAnalysisReport(String text, String description, boolean smellFound) {
    super(text, description, smellFound);
  }
  public UnsupportedAnalysisReport(String analyzerName) {
    this(analyzerName,description,false);
  }  
}