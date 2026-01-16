package edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.featurescatter;

import com.google.auto.service.AutoService;
import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.ElementVisitor;
import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.IAnalyzer;
import edu.kit.kastel.sdq.case4lang.refactorlizar.analyzer.api.Report;
import edu.kit.kastel.sdq.case4lang.refactorlizar.model.ModularLanguage;
import spoon.reflect.CtModel;
import spoon.reflect.declaration.CtElement;
import spoon.reflect.declaration.CtPackage;

@AutoService(IAnalyzer.class)
public class FeatureScatterAnalyzer implements IAnalyzer {
  
  private ModularLanguage language;
  private CtModel model;
  public FeatureScatterAnalyzer() {
    
  }
  @Override
  public Report analyze(CtElement element) {
    PackageVisitor visitor = new PackageVisitor(language, model);
    element.accept(visitor);
    return visitor.getReport();
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
    return "FeatureScatterAnalyzer";
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