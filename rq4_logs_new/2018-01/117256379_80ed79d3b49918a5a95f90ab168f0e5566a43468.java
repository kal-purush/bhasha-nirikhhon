package org.cloudfoundry.test;

public class MyType

{
  private String name1;
  private String name2;

  public MyType() {

  }

  public void setName1(String name1) {
    this.name1 = name1;
  }

  public String getName1() {
    return name1;
  }

  public void setName2(String name2) {
    this.name2 = name2;
  }

  public String getName2() {
    return name2;
  }

  public String toString() {
    return "name1 = " + name1 + " and name2 = " + name2;
  }
}