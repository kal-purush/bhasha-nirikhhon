using System.Collections;
using System.Collections.Generic;
using Palmmedia.ReportGenerator.Core.Parser.Analysis;
using Unity.VisualScripting.Dependencies.NCalc;
using UnityEngine;

public enum PlayerElementType
{
    Logi, // Fire
    Kari, // Wind
    Hler // Water
}

public class PlayerElement : MonoBehaviour
{
    private SpriteRenderer SpriteRenderer;
    [field: SerializeField]
    public PlayerElementType ElementType { get; private set; }

    void Awake()
    {
        this.SpriteRenderer = GetComponent<SpriteRenderer>();
    }

    // Start is called before the first frame update
    void Start()
    {
        this.SpriteRenderer.color = InitElementColor(this.ElementType);
    }

    // Update is called once per frame
    void Update()
    {

    }

    static private Color InitElementColor(in PlayerElementType elementType)
    => elementType switch
    {
        PlayerElementType.Logi => new Color(0.8f, 0.3f, 0.1f),
        PlayerElementType.Kari => new Color(0.8f, 0.9f, 0.9f),
        PlayerElementType.Hler => new Color(0.0f, 0.4f, 0.5f),
        _ => throw new System.NotImplementedException(),
    };
}