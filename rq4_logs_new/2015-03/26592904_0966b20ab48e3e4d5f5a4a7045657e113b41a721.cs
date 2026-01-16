using UnityEngine;
using System.Collections;
using UnityEngine.UI;

namespace Wardraft.UI {

  public class GameUIController : MonoBehaviour {
  
    GameObject SelectedInfo;
    
    void Start () {
      SelectedInfo = GameObject.Find("SelectedInfo");
      HideInfo();
    }
    
    public void ShowInfo (System.Object toShow) {
      if (toShow is ActiveActor) {
        ActiveActor aa = toShow as ActiveActor;
        SelectedInfo.SetActive(true);
        string owned = "";
        if (aa.ownerID == GameData.PlayerID) owned = " (Owned)";
        changeSelectedLabel("LabelName", Actors.codes[aa.code] + owned);
        changeSelectedLabel("LabelHealth", "Health: " + aa.attributes.health.current.ToInt() + "/" + aa.attributes.health.max.ToInt());
        changeSelectedLabel("LabelDamage", "Damage: " + aa.attributes.damage.current.ToInt());
        changeSelectedLabel("LabelArmor", "Armor: " + aa.attributes.armor.current.ToInt());
        changeSelectedLabel("LabelSpeed", "Speed: " + aa.attributes.speed.current.ToInt());
        changeSelectedLabel("LabelMana", "Mana: " + aa.attributes.mana.current.ToInt() + "/" + aa.attributes.mana.max.ToInt());
        changeSelectedLabel("LabelAttackRange", "A. Range: " + aa.attributes.attackRange.current);
      }
    }
    
    public void HideInfo () {
      SelectedInfo.SetActive(false);
    }
    
    void changeSelectedLabel (string name, string text) {
      GameObject textObject = SelectedInfo.transform.FindChild(name).gameObject;
      changeText(textObject, text);
    }
    
    void changeText (GameObject textObject, string text) {
      textObject.GetComponent<Text>().text = text;
    }
  
  }

}