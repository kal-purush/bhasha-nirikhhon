package consensus;

import java.io.Serializable;

public class ConsensusAppendRequest implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -8072318929579338694L;
  
  // Attributes
  private Integer leaderTerm;
  private String leaderAddress;
  private Integer leaderPort;
 
  // Constructor
  public ConsensusAppendRequest(Integer leaderTerm, String leaderAddress, Integer leaderPort) {
    this.leaderTerm = leaderTerm;
    this.leaderAddress = leaderAddress;
    this.leaderPort = leaderPort;
  }

  // Getters & Setters
  public Integer getLeaderTerm() {
    return leaderTerm;
  }

  public void setLeaderTerm(Integer leaderTerm) {
    this.leaderTerm = leaderTerm;
  }

  public String getLeaderAddress() {
    return leaderAddress;
  }

  public void setLeaderAddress(String leaderAddress) {
    this.leaderAddress = leaderAddress;
  }
  
  public Integer getLeaderPort() {
    return leaderPort;
  }
  
  public void setLeaderPort(Integer leaderPort) {
    this.leaderPort = leaderPort;
  }
}