package edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.languageblob;

import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.ElementVisitor;
import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.IAnalyzer;
import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.Report;
import edu.kit.kastel.sdq.case4lang.refactorlizar.model.ModularLanguage;
import spoon.reflect.CtModel;
import spoon.reflect.declaration.CtElement;
import spoon.reflect.declaration.CtPackage;

public class LanguageBlobAnalyzer implements IAnalyzer {
  
  private ModularLanguage language;
  private CtModel model;
  @Override
  public Report analyze(CtElement element) {
    // jetzt graph bauen mit kanten von sprachfeature -> simulatorpackage. Dann schauen dass jedes Simulatorpaket nur incoming edges = 1 hat.
    return null;
  }

  @Override
  public void init(ModularLanguage language, CtModel simulatorAST) {
    this.language = language;
    this.model = simulatorAST;
  }
  @Override
  public String getDescription() {
    return "";
  }
  @Override
  public String getName() {
    return "LanguageBlobAnalyzer";
  }

  @Override
  public boolean canAnalyze(CtElement element) {
    ElementVisitor visitor = new ElementVisitor(){
      @Override
      public void visitCtPackage(CtPackage arg0) {
        this.setResult(true);    
      }
    };
    element.accept(visitor);
    return visitor.canVisit();
  }
}