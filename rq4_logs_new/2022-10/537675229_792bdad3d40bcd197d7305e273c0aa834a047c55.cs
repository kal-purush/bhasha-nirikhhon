
using System.Collections.Generic;
using JetBrains.Annotations;
using UnityEngine;

public class FixedItemSpawner : MonoBehaviour, IItemSpawner {

    public ItemSpawnerVariables vars;
    
    // stage 따라 다르게 받도록 수정
    private GameObject[] items;
    private Vector2[] points;
    
    // key : item
    public Dictionary<Vector2[], string> ItemPointDic;

    private void Start() {
        for (int i = 0; i < items.Length; i++) {
            
        }
    }

    private void Update() {
    }
    
    public void Spawn() {

        if (GameManager.instance.isGameOver) {
            return;
        }
        
        
        
    }


}