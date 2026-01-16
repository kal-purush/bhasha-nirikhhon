package org.folio.rest.workflow.model;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;

import org.folio.rest.workflow.model.components.DelegateTask;

@Entity
public class DatabaseConnectionTask extends Node implements DelegateTask {

  @ElementCollection
  private Set<EmbeddedVariable> inputVariables;

  @Embedded
  private EmbeddedVariable outputVariable;

  @Column(nullable = false)
  private boolean asyncBefore;

  @Column(nullable = false)
  private boolean asyncAfter;

  @Column(nullable = false)
  private String identifier;

  @Column(nullable = false)
  private String url;

  @Column(nullable = true)
  private String user;

  @Column(nullable = true)
  private String password;

  public DatabaseConnectionTask() {
    super();
    asyncBefore = false;
    asyncAfter = false;
  }

  public Set<EmbeddedVariable> getInputVariables() {
    return inputVariables;
  }

  public void setInputVariables(Set<EmbeddedVariable> inputVariables) {
    this.inputVariables = inputVariables;
  }

  public EmbeddedVariable getOutputVariable() {
    return outputVariable;
  }

  public void setOutputVariable(EmbeddedVariable outputVariable) {
    this.outputVariable = outputVariable;
  }

  public boolean isAsyncBefore() {
    return asyncBefore;
  }

  public void setAsyncBefore(boolean asyncBefore) {
    this.asyncBefore = asyncBefore;
  }

  public boolean isAsyncAfter() {
    return asyncAfter;
  }

  public void setAsyncAfter(boolean asyncAfter) {
    this.asyncAfter = asyncAfter;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

}