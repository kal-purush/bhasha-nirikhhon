using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ToggleHelpPopups : MonoBehaviour
{
	// Update is called once per frame
	void Update ()
    {
        GameManager.Instance.helpPopupsEnabled = GetComponent<Toggle>().isOn;
	}
}