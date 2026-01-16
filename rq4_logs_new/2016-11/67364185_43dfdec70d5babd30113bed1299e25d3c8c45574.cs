using UnityEngine;
using System.Collections;

public class CharacterView : MonoBehaviour {

    protected UIController ui;
    protected CharacterModel model;

    protected virtual void Start()
    {
        model = new CharacterModel();
        ui = FindObjectOfType<UIController>();
    }

    public void takeDamage(int dam)
    {
        model.takeDamage(dam);
        ui.thingTookDamage(transform.position, dam);
    }
}