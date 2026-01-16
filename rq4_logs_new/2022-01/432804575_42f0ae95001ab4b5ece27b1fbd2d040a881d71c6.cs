using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DestroyMM : MonoBehaviour
{
    void Start()
    {
        Destroy(GameObject.Find("MusicManager"));
    }
}