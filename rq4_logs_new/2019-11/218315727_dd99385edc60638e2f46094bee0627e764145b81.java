package comunication;

import java.io.Serializable;

public class ComunicationMessage implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -7581809375014948234L;

  /*
   * Attributes
   */
  private String message;
  private Integer term;
  private FullAddress fullAddress;

  /*
   * Follower Constructor
   */

  public ComunicationMessage(String message, Integer term, FullAddress fullAddress) {
    this.message = message;
    this.term = term;
    this.fullAddress = fullAddress;
  }

  /*
   * Getters & Setters
   */
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Integer getTerm() {
    return term;
  }

  public void setTerm(Integer term) {
    this.term = term;
  }

  public FullAddress getFullAddress() {
    return fullAddress;
  }

  public void setFullAddress(FullAddress fullAddress) {
    this.fullAddress = fullAddress;
  }

  /*
   * String toString
   */
  @Override
  public String toString() {
    return this.getMessage() + "," + this.getTerm() + "," + this.getFullAddress();
  }
}