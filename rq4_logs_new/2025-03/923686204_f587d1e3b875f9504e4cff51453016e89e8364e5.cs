using Unity.AI.Navigation;
using UnityEngine;
using UnityEngine.AI;

[RequireComponent(typeof(NavMeshSurface))]
public class NavMeshManager : MonoBehaviour
{
    private NavMeshSurface Surface;

    private static NavMeshManager _Instance;
    public static NavMeshManager Instance
    {
        get
        {
            return _Instance;
        }

        private set
        {
            _Instance = value;
        }
    }

    private void Awake()
    {
        if (Instance != null)
        {
            Destroy(gameObject);
            return;
        }
        else {
            Instance = this;
        }

        Surface = GetComponent<NavMeshSurface>();
    }

    public void BakeNavMesh()
    {
        Surface.BuildNavMesh();
    }
}