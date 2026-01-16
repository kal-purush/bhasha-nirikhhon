/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package trafficsimulator.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author balazs
 */
public class Junction {
  private HashMap<Lane, List<Lane>> connections;
  
  public Junction(){
    connections = new HashMap<>();
  }
  
  public void connect(Lane source, Lane destination){
    if(!connections.containsKey(source)){
      connections.put(source, new ArrayList<Lane>());
    }
    List<Lane> lanes = connections.get(source);
    lanes.add(destination);
    source.setJunction(this);
  }
  
  public List<Lane> getConnectedLanes(Lane lane){
    return connections.get(lane);
  }
}