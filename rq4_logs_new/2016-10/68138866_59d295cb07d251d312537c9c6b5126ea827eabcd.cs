using UnityEngine;
using UnityEngine.VR.WSA;

public class SpatialMapping : MonoBehaviour {

    public static SpatialMapping Instance { private set; get; }

    [HideInInspector]
    public static int PhysicsRaycastMask;

    [Tooltip("The material to use when rendering Spatial Mapping data.")]
    public Material DrawMaterial;

    [Tooltip("If true, the Spatial Mapping data will be rendered.")]
    public bool drawVisualMeshes = false;

    private bool mappingEnabled = true;

    private int physicsLayer = 31;

    private SpatialMappingRenderer spatialMappingRenderer;

    private SpatialMappingCollider spatialMappingCollider;

    public bool DrawVisualMeshes
    {
        get
        {
            return drawVisualMeshes;
        }
        set
        {
            drawVisualMeshes = value;

            if (drawVisualMeshes)
            {
                spatialMappingRenderer.currentRenderSetting = SpatialMappingRenderer.RenderSetting.CustomMaterial;
                spatialMappingRenderer.customMaterial = DrawMaterial;
            }
            else
            {
                spatialMappingRenderer.currentRenderSetting = SpatialMappingRenderer.RenderSetting.None;
            }
        }
    }

    public bool MappingEnabled
    {
        get
        {
            return mappingEnabled;
        }
        set
        {
            mappingEnabled = value;
            spatialMappingCollider.freezeUpdates = !mappingEnabled;
            spatialMappingRenderer.freezeUpdates = !mappingEnabled;
            gameObject.SetActive(mappingEnabled);
        }
    }

    void Awake()
    {
        Instance = this;
    }

    void Start()
    {
        spatialMappingRenderer = gameObject.GetComponent<SpatialMappingRenderer>();
        spatialMappingRenderer.surfaceParent = this.gameObject;
        spatialMappingCollider = gameObject.GetComponent<SpatialMappingCollider>();
        spatialMappingCollider.surfaceParent = this.gameObject;
        spatialMappingCollider.layer = physicsLayer;
        PhysicsRaycastMask = 1 << physicsLayer;
        DrawVisualMeshes = drawVisualMeshes;
        MappingEnabled = mappingEnabled;
    }
}