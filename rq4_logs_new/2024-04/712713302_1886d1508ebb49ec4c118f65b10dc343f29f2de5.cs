using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Level1Manager : MonoBehaviour
{
    void Start(){
        StartCoroutine(initializeSequence());
    }

    IEnumerator initializeSequence(){
        yield return new WaitForSeconds(2f);
        PlayerAspects.instance.loadDoors();
    }
}