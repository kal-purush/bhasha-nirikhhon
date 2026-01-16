using UnityEngine;
using System.Collections;

namespace Wardraft.Game {

  public class ActiveActorController : MonoBehaviour {
  
    public ActiveActorVM AAVM;
    public ActiveActor AA;
    
    public void HoverOn () {
      if (!AA.selected) {
        if (AA.ownerID != GameData.PlayerID) AAVM.MouseoverEnemy();
        else AAVM.MouseoverOwn();
      }
    }
    
    public void HoverExit () {
      if (!AA.selected) {
        AAVM.Normal();
      }
    }
    
    public void Select () {
      if (AA.ownerID != GameData.PlayerID) AAVM.SelectEnemy();
      else AAVM.SelectOwn();
      AA.Select();
      PlayerController.yourself.Select(this);
    }
    
    public void Deselect () {
      AAVM.Normal();
      AA.Deselect();
    }
  
  }

}