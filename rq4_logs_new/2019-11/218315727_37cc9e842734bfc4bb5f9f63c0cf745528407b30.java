package consensus;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ConsensusRequestInterface extends Remote {
  /**
   * This function should be called remotely to make an RPC AppendRequest. Normally, the leaders
   * invoke this method on all replicas.
   * 
   * @param request Contains the ConsensusAppendRequest with all information from the leader.
   * 
   * @return Boolean Returns true if it accepted leader RPC, false otherwise.
   * 
   * @throws RemoteException When it fails to reach the host.
   * 
   * @see RemoteException
   * 
   */
  Boolean appendRequest(ConsensusAppendRequest request) throws RemoteException;

  /**
   * This function should be called remotely to make an RPC VoteRequest. Normally, the candidates
   * invoke this method on all replicas.
   * 
   * @param request Contains the ConsensusVoteRequest with all information from the candidate.
   * 
   * @return Boolean Returns true if it accepted candidate RPC, false otherwise.
   * 
   * @throws RemoteException When it fails to reach the host.
   * 
   * @see RemoteException
   * 
   */
  Boolean voteRequest(ConsensusVoteRequest request) throws RemoteException;
}