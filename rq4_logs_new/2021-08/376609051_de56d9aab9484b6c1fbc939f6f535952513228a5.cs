using UnityEngine;

public class Map : MonoBehaviour
{
   private Transform _player;
   [Header("Смещение камеры (минимапа)")]
   public Vector3 _vector3;

    private void Start()
    {
        _player = Camera.main.transform;
        _player.parent = null;
        _player.position = transform.position;
        var rt = Resources.Load<RenderTexture>("MapTex");
        GetComponent<Camera>().targetTexture = rt;

    }

    private void LateUpdate()
    {
        var newPosition = _player.position;
        newPosition.y = transform.position.y;
        transform.position = Camera.main.transform.position + _vector3;
        transform.rotation =  Quaternion.Euler(90, 0, 0);
    }
}