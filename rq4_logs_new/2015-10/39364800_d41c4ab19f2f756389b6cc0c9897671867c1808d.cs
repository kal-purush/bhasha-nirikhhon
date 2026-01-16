// **************************************************************
// **** Created on 10/08/15 by Kevin Means
// **** 1.) Allows tracking of generic objects
// **************************************************************
using UnityEngine;
using System.Collections;

public class TrackingProperties : MonoBehaviour {
  #region Public Fields + Properties + Events + Delegates + Enums

  public bool isFound = false;

  #endregion Public Fields + Properties + Events + Delegates + Enums

  public bool Find()
  {
    if(isFound == false)
    {
      isFound = true;
      return true;
    }
    return false;
  }
}