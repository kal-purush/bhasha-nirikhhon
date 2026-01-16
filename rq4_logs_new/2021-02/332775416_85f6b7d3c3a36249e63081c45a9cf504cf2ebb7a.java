package edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.dependencydirection;

import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.IAnalyzer;
import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.Report;
import edu.kit.kastel.sdq.case4lang.refactorlizar.model.ModularLanguage;
import edu.kit.kastel.sdq.case4lang.refactorlizar.model.SimulatorModel;
import spoon.reflect.declaration.CtElement;

public class DependencyDirectionAnalyzer implements IAnalyzer {

  private ModularLanguage language;
  private SimulatorModel simulatorAST;
  @Override
  public Report analyze(CtElement element) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void init(ModularLanguage language, SimulatorModel simulatorAST) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getDescription() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean canAnalyze(CtElement element) {
    // TODO Auto-generated method stub
    return false;
  }

}