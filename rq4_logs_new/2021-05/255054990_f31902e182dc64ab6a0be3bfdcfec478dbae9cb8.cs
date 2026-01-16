using System;
using System.Collections.Generic;
using UnityEngine;

public class CharacterStyleSetup : MonoBehaviour {
    [Header("Settings")]
    [SerializeField]
    private List<GameObject> hairs;

    [SerializeField]
    private List<GameObject> shirts;

    [SerializeField]
    private List<GameObject> pants;

    [SerializeField]
    private List<GameObject> shoes;

    private int currentHairIdx;
    private int currentShirtIdx;
    private int currentPantIdx;
    private int currentShoesIdx;
    
    private Color currentHairColor = Color.white;
    private Color currentShirtColor = Color.white;
    private Color currentPantColor = Color.white;
    private Color currentShoesColor = Color.white;

    private void Start() {
        SelectPart(CharacterPartType.HAIR, 0);
        SelectPart(CharacterPartType.SHIRT, 0);
        SelectPart(CharacterPartType.PANT, 0);
        SelectPart(CharacterPartType.SHOES, 0);
    }

    public void ApplyColor(CharacterPartType partType, Color color) {
        List<GameObject> choices = GetPartList(partType);

        Renderer currentPart = choices[this.GetCurrentPartIdx(partType)].GetComponent<Renderer>();
        Material newMaterial = currentPart.material;
        newMaterial.color = color;
        currentPart.material = newMaterial;
        
        switch (partType) {
            case CharacterPartType.HAIR:
                this.currentHairColor = color;
                break;

            case CharacterPartType.PANT:
                this.currentPantColor = color;
                break;

            case CharacterPartType.SHIRT:
                this.currentShirtColor = color;
                break;

            case CharacterPartType.SHOES:
                this.currentShoesColor = color;
                break;
        }
    }

    public void SelectPart(CharacterPartType partType, int idx) {
        List<GameObject> choices = GetPartList(partType);

        if (idx < 0) {
            idx = choices.Count - 1;
        } else if (idx >= choices.Count) {
            idx = 0;
        }

        for (int i = 0; i < choices.Count; i++) {
            choices[i].SetActive(i == idx);
        }

        switch (partType) {
            case CharacterPartType.HAIR:
                this.currentHairIdx = idx;
                this.ApplyColor(partType, this.currentHairColor);
                break;

            case CharacterPartType.PANT:
                this.currentPantIdx = idx;
                this.ApplyColor(partType, this.currentPantColor);
                break;

            case CharacterPartType.SHIRT:
                this.currentShirtIdx = idx;
                this.ApplyColor(partType, this.currentShirtColor);
                break;

            case CharacterPartType.SHOES:
                this.currentShoesIdx = idx;
                this.ApplyColor(partType, this.currentShoesColor);
                break;
        }
        
    }

    public int GetCurrentPartIdx(CharacterPartType partType) {
        switch (partType) {
            case CharacterPartType.HAIR:
                return this.currentHairIdx;

            case CharacterPartType.PANT:
                return this.currentPantIdx;

            case CharacterPartType.SHIRT:
                return this.currentShirtIdx;

            case CharacterPartType.SHOES:
                return this.currentShoesIdx;
        }

        throw new Exception("[CharacterStyleSetup] Unknown part !");
    }

    public static CharacterPartType GetCharacterPartType(string name) {
        switch (name) {
            case "HAIR":
                return CharacterPartType.HAIR;

            case "PANT":
                return CharacterPartType.PANT;

            case "SHIRT":
                return CharacterPartType.SHIRT;

            case "SHOES":
                return CharacterPartType.SHOES;
        }

        throw new Exception("[CharacterStyleSetup] Unknown part !");
    }

    private List<GameObject> GetPartList(CharacterPartType partType) {
        switch (partType) {
            case CharacterPartType.HAIR:
                return hairs;

            case CharacterPartType.PANT:
                return pants;

            case CharacterPartType.SHIRT:
                return shirts;

            case CharacterPartType.SHOES:
                return shoes;
        }

        throw new Exception("[CharacterStyleSetup] Unknown part !");
    }
}