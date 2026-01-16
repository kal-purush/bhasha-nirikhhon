using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using VolumetricLines;

public class LightSaber : MonoBehaviour {

    [Range(0, 2f)]
    public float lightSaberLength = 0.7f;
    private VolumetricLineBehavior line;

    void Awake() {
        line = GetComponent<VolumetricLineBehavior>();
    }

    // Use this for initialization
    void Start() {
        line.EndPos = new Vector3(0, 0, lightSaberLength);
    }

    // Update is called once per frame
    void Update() {

    }
}