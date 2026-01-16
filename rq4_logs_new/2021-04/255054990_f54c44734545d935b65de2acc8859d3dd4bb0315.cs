using UnityEngine;

public class GeographicArea : MonoBehaviour {
    [Header("Settings")]
    [SerializeField]
    private string locationText;
    
    public string LocationText {
        get => locationText;
        set => locationText = value;
    }
}