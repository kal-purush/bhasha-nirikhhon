using UnityEngine;
using System.Collections;

public class ViewAspectController : MonoBehaviour
{

    [SerializeField]
    Vector2 aspect = new Vector2(9, 16);

    [SerializeField]
    Color backgroundColor = Color.black;

    private float aspectRate = 0.0f;
    private Camera camera = null;
    private static Camera backgroundCamera = null;

    void Start()
    {
        aspectRate = (float)aspect.x / aspect.y;
        camera = Camera.main;

        CreateBackgroundCamera();
        UpdateScreenRate();

    }

    void CreateBackgroundCamera()
    {
#if UNITY_EDITOR
        if (!UnityEditor.EditorApplication.isPlaying)
            return;
#endif

        if (backgroundCamera != null)
            return;

        var backGroundCameraObject = new GameObject("Background Color Camera");
        backgroundCamera = backGroundCameraObject.AddComponent<Camera>();
        backgroundCamera.depth = -99;
        backgroundCamera.fieldOfView = 1;
        backgroundCamera.farClipPlane = 1.1f;
        backgroundCamera.nearClipPlane = 1;
        backgroundCamera.cullingMask = 0;
        backgroundCamera.depthTextureMode = DepthTextureMode.None;
        backgroundCamera.backgroundColor = backgroundColor;
        backgroundCamera.renderingPath = RenderingPath.VertexLit;
        backgroundCamera.clearFlags = CameraClearFlags.SolidColor;
        backgroundCamera.useOcclusionCulling = false;
        backGroundCameraObject.hideFlags = HideFlags.NotEditable;
    }

    void UpdateScreenRate()
    {
        float baseAspect = aspect.y / aspect.x;
        float nowAspect = (float)Screen.height / Screen.width;

        if (baseAspect > nowAspect)
        {
            var changeAspect = nowAspect / baseAspect;
            camera.rect = new Rect((1 - changeAspect) * 0.5f, 0, changeAspect, 1);
        }
        else
        {
            var changeAspect = baseAspect / nowAspect;
            camera.rect = new Rect(0, (1 - changeAspect) * 0.5f, 1, changeAspect);
        }
    }

    bool IsChangeAspect()
    {
        return camera.aspect == aspectRate;
    }

    void Update()
    {
        if (IsChangeAspect())
            return;

        UpdateScreenRate();
        camera.ResetAspect();
    }

}