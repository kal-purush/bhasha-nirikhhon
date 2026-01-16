using UnityEngine;
using System.Collections;

public class ActiveActorVM : MonoBehaviour {

  public bool selected;
  Renderer modelRenderer;
  Shader normalShader, outlineShader;
  
  void Start () {
    normalShader = Shader.Find("Diffuse");
    outlineShader = Shader.Find("Outlined/Silhouetted Bumped Diffuse");
    modelRenderer = transform.GetComponentInChildren<Renderer>();
  }
  
  public void OnMouseOver () {
    if (!selected) {
      mouseover(modelRenderer);
    }
  }
  
  public void OnMouseExit () {
    if (!selected) {
      normal(modelRenderer);
    }
  }
  
  public void OnMouseClick () {
    GameObject actors = GameObject.Find("Game/Actors");
    for (int i=0; i<actors.transform.childCount; i++) {
      Transform child = actors.transform.GetChild(i);
      if (child != this.transform) {
        normal(child.transform.Find("Model").renderer);
        ActiveActorVM aavm = child.GetComponent<ActiveActorVM>();
        if (aavm != null) aavm.selected = false;
      }
    }
    select(modelRenderer);
  }
  
  void normal (Renderer renderer) {
    renderer.material.shader = normalShader;
    selected = false;
  }
  
  void mouseover (Renderer renderer) {
    renderer.material.shader = outlineShader;
    renderer.material.SetColor("_OutlineColor", new Color(.5f,1f,1f,.5f));
    
  }
  
  void select (Renderer renderer) {
    selected = true;
    renderer.material.shader = outlineShader;
    renderer.material.SetColor("_OutlineColor", new Color(0f,1f,0f,.5f));
  }
  
  

}