using UnityEngine;
using System.Collections;

public class DialogueBox : MonoBehaviour {
  public bool dialogue;
  public string text;
  public Vector2 textboxPos = new Vector2();
  
  void Start () {
    
  }
  
  void Update () {
    
  }
  
  void OnGUI() {
    if(dialogue == true) {
      GUI.BeginGroup (new Rect (Screen.height - textboxPos.x, Screen.width - textboxPos.y, textboxPos.x, textboxPos.y));
        GUI.Box(new Rect(0,0,textboxPos.x,textboxPos.y),text);
        GUI.Label (new Rect(0,0,textboxPos.x,textboxPos.y), text);
      GUI.EndGroup ();
    }
  }
}