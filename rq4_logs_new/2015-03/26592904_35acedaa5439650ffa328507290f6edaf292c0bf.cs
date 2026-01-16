using UnityEngine;
using System.Collections;

namespace Wardraft.Game {

  public class TileController : MonoBehaviour {
  
    public TileViewModel TVM;
    public Tile tile;
    
    public void HoverOn () {
      if (PlayerController.yourself.selected != this) {
        TVM.Mouseover();
      }
    }
    
    public void HoverExit () {
      if (PlayerController.yourself.selected != this) {
        TVM.Normal();
      }
    }
    
    public void Select () {
      TVM.Select();
      PlayerController.yourself.Select(this);
    }
    
    public void Deselect () {
      TVM.Normal();
    }
  
  }

}