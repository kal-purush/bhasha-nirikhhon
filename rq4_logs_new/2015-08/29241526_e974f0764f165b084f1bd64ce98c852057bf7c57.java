package org.everit.osgi.ecm.component.webconsole;

/**
 * Class for storing the sum of component states.
 *
 */
public class ComponentStateSum {

  private int active = 0;

  private int failed = 0;

  private int inactive = 0;

  private int starting = 0;

  private int stopping = 0;

  private int unsatisfied = 0;

  public int getActive() {
    return active;
  }

  public int getFailed() {
    return failed;
  }

  public int getInactive() {
    return inactive;
  }

  public int getStarting() {
    return starting;
  }

  public int getStopping() {
    return stopping;
  }

  public int getUnsatisfied() {
    return unsatisfied;
  }

  public void setActive(final int active) {
    this.active = active;
  }

  public void setFailed(final int failed) {
    this.failed = failed;
  }

  public void setInactive(final int inactive) {
    this.inactive = inactive;
  }

  public void setStarting(final int starting) {
    this.starting = starting;
  }

  public void setStopping(final int stopping) {
    this.stopping = stopping;
  }

  public void setUnsatisfied(final int unsatisfied) {
    this.unsatisfied = unsatisfied;
  }

}