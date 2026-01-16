using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using Hex;

namespace GameUI
{
    public class CityLabelManager : MonoBehaviour
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
            var pos = HexCellService.GetTranslationComponentByHexCoordinates(CoordinatesService.CreateFromOffset(8, 2));

            label.GetComponent<RectTransform>().SetParent(gridCanvas.transform, false);
            label.GetComponent<RectTransform>().position = new Vector3(pos.x, pos.y + 10f, pos.z - 5f);
        }

        // Update is called once per frame
        void Update()
        {

        }
    }
}