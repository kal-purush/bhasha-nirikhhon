using Unity.Entities;
using UnityEngine;
using UnityEngine.UI;
using Hex.Cell;
using Hex.Coordinates;

namespace GameUI.Components
{
    public class CityLabelManager : MonoBehaviour
    {
        public GameObject cityPanelPrefab;
        Canvas gridCanvas;

        EntityQuery query;

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