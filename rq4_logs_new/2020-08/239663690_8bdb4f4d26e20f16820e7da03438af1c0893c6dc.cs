using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using Hex;

public class CityPanelManager : MonoBehaviour
{
    public GameObject cityPanelPrefab;
    Canvas gridCanvas;

    void Awake()
    {
        gridCanvas = GetComponentInChildren<Canvas>();
    }

    void Start()
    {
        GameObject label = Instantiate(cityPanelPrefab);
        label.GetComponent<RectTransform>().SetParent(gridCanvas.transform, false);
        label.GetComponent<RectTransform>().anchoredPosition3D = new Vector3(
            PositionCalculator.GetPositionX(17, 8),
            PositionCalculator.GetPositionZ(8),
            220f
        );
    }

    // Update is called once per frame
    void Update()
    {

    }
}