using UnityEngine;
using System.Collections;
using System.Collections.Generic;

[System.Serializable]
public class EventClass : Tutorial.SwitchOnOff {
  public UIObj[] UIObjects;
  private bool enabled = false;
  public EventObj[] EventObjects;
  public bool dialogue;
  public string text;
  //private List<GameObject> spawnedObjects;

  void Start () {
    enabled = false;
    //spawnedObjects = new List<GameObject>();
  }

  [System.Serializable]
  public struct UIObj {
    public GameObject EventObject;
    public bool flag;
  }

  [System.Serializable]
  public struct EventObj {
    public GameObject EventObject;
    public List<Vector3> spawnLocations;
  }

  public void enable () {
    for (int i = 0; i < UIObjects.Length; i++) {
      if (UIObjects [i].EventObject != null) {
        Tutorial.SwitchOnOff Interface = (Tutorial.SwitchOnOff)UIObjects [i].EventObject.GetComponent<Tutorial.SwitchOnOff> ();

        if (UIObjects [i].flag == false) {
          Interface.disable ();
        } else {
          Interface.enable ();
        }
      }
    }

    if (enabled == false) {
      for (int i = 0; i < EventObjects.Length; i++) {
        for (int j = 0; j < EventObjects[i].spawnLocations.Count; j++) {
          //GameObject clone = 
          GameObject.Instantiate (EventObjects [i].EventObject, EventObjects [i].spawnLocations [j], Quaternion.identity);// as GameObject;
          //GameObject clone = Resources.Load(EventObjects[i].EventObject.name) as GameObject;
          //spawnedObjects.Add (clone);
        }
      }
    }
    enabled = true;
  }
  
  public void render () {
    //enable ();
    if (dialogue == true) {
       
    }
  }
  
  public void disable () {
    if (enabled == true) {
      /*for (int i = 0; i < spawnedObjects.Count; i++) {
        GameObject obj = spawnedObjects [i];
        spawnedObjects.RemoveAt (i);
        MonoBehaviour.Destroy (obj);
      }*/
    }

    for (int i = 0; i < UIObjects.Length; i++) {
      if (UIObjects [i].EventObject != null) {
        Tutorial.SwitchOnOff Interface = (Tutorial.SwitchOnOff)UIObjects [i].EventObject.GetComponent<Tutorial.SwitchOnOff> ();
        if (UIObjects [i].flag == true) {
          Interface.disable ();
        } else {
          Interface.enable ();
        }
      }
    }
    enabled = false;
  }
}