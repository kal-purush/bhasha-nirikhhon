using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Balanced : MonoBehaviour
{
    public int numStars; 
    public int numLoverStar; 
    public bool isEqual; 
    
  void Update() {
    GameObject[] currNumStars = GameObject.FindGameObjectsWithTag("pStar"); 
    numStars = currNumStars.Length; 
    GameObject[] currNumLoveStars = GameObject.FindGameObjectsWithTag("loverStar"); 
    numLoverStar = currNumLoveStars.Length; 
    
    equalAmountStars(); 
    isZero(); 
  }

  public bool equalAmountStars() {
    if(numStars == numLoverStar) {
      isEqual = true; 
      return isEqual; 
    } else {
      isEqual = false; 
      return isEqual; 
    }
    
  }

  public bool isZero() {
    if(numStars == 0 && numLoverStar == 0) {
      Debug.Log(" they are zero: True");
      return true; 
    } else {
      Debug.Log(" they're NOT zero: False");
      return false;
    }
  }
}