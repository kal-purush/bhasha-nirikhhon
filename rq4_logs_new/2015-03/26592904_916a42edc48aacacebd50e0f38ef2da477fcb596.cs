using UnityEngine;
using System.Collections;

namespace Wardraft.Game {

  public class MapData : MonoBehaviour {
  
    public Vector4 CameraBounds;
    public string MapName;
  
    public static MapData current;
  
    void Start () {
      checkDependencies();
      current = this;
    }
    
    void checkDependencies() {
      if (CameraBounds == Vector4.zero) Debug.LogError("Please set camera bounds in Map Data");
      if (MapName == "") Debug.LogError("Please set map name in Map Data");
    }
  
  }

}