package org.bzs.enculette;

/**
 * TODO apaquin This type ...
 *
 * @author apaquin
 */
public class HelloWorld {
  private int call;

  /**
   * The constructor.
   */
  public HelloWorld() {
  }

  /**
   * The constructor.
   *
   * @param num
   */
  public HelloWorld(int num) {
    this.call = 0;
  }

  /**
   * @return une string contenant la chaine "Hello"
   */
  public String sayhello() {

    return "Hello";
  }

  /**
   * @return le nombre de fois où la fonction sayhello a été appelé
   */
  public int countCall() {

    return this.call;
  }
}