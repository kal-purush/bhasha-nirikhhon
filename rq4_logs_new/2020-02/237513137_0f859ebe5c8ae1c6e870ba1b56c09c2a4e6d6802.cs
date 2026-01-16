using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
public class BallonDash : MonoBehaviour
{
    // Start is called before the first frame update
    private TextMeshProUGUI text;
    private Baloon balloon;
    private Rigidbody carrtRigidbody;

    void Start()
    {
        text = GetComponent<TextMeshProUGUI>();
        var baseBalloon = GameObject.FindGameObjectWithTag("Baloon");
        balloon = baseBalloon.GetComponentInParent<Baloon>();
        carrtRigidbody = GameObject.Find("Platform").GetComponent<Rigidbody>();
    }

    // Update is called once per frame
    void Update()
    {
        text.text = "Temp C: " + balloon.GetTempC() + "\n" +
                    "Altitude: " + carrtRigidbody.position.y;
    }
}