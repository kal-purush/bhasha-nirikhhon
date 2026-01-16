using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class UILayerObject : MonoBehaviour
{
    [SerializeField]
    private string layerName = "";
    public string LayerName
    {
        get
        {
            return layerName;
        }
    }

    void Start()
    {
        Image image = GetComponent<Image>();
        if (image != null)
            UIManager.Instance.AddObjectOnLayer(layerName, image);
        else
            Debug.LogError("This Object DOESN'T HAVE an Image Component");
    }
}