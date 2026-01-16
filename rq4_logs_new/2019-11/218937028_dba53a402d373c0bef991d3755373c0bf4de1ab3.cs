using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CreateViewManager : MonoBehaviour
{
    public GameObject characterPrefab;

    private Transform characterTransform;
    private CharacterSettingController characterSettingController;

    public void createCharacter() {
        GameObject character = Instantiate(characterPrefab);

        characterTransform = character.GetComponent<Transform>();
        characterSettingController = character.GetComponent<CharacterSettingController>();

        characterTransform.position = new Vector3(1.2f, 4.3f, -4);

        characterSettingController.setInitState();
    }

    public void setField(int field) {
        characterSettingController.setField(field);
    }

    public void setSaturation(float saturation) {
        characterSettingController.setSaturation(saturation);
    }

    public void setAcc(int year) {
        characterSettingController.setAcc(year);
    }
}