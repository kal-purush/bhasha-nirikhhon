using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MinecraftGenerator : MonoBehaviour
{
    [SerializeField] [Range(0.1f, 1f)] private float precision;
    [SerializeField] private Layer[] layers;
    [SerializeField] private int depth;
    [SerializeField] private int gridSize;
    [SerializeField] private Transform parent;

    private Vector3[,] superficie;
    private List<List<Layer>> layerList;

    [System.Serializable]
    private class Layer
    {
        [SerializeField] private int minDepth, maxDepth;
        [SerializeField] private GameObject prefab;
        [SerializeField] [Range(0f, 1f)] private float probability;

        public int GetMinDepth() { return minDepth; }
        public float GetProbability() { return probability; }
        public int GetMaxDepth() { return maxDepth; }
        public GameObject GetPrefab() { return prefab; }
    }

    // Start is called before the first frame update
    void Start()
    {
        GenerateDepthLayers();
        superficie = new Vector3[gridSize, gridSize];
        GenerateLayers();
    }

    // Update is called once per frame
    void Update()
    {
     
    }

    private void GenerateDepthLayers()
    {
        layerList = new List<List<Layer>>();
        for (int x = 0; x < depth; x++)
        {
            var list = new List<Layer>();
            foreach(Layer lay in layers)
            {
                if (lay.GetMinDepth() <= x && x <= lay.GetMaxDepth())
                    list.Add(lay);
            }
            layerList.Add(list);
        }
    }

    private GameObject GetObjectByLayerList(int pos)
    {
        var list = layerList[pos];
        GameObject obj = null;
        while(obj == null)
        {
            var layer = list[Random.Range(0, list.Count)];
            if (Random.value < layer.GetProbability())
                obj = layer.GetPrefab();
        }
        
        return obj;
    }

    private void GenerateLayers()
    {
        for (int x = 0; x < gridSize; x++)
        {
            for (int z = 0; z < gridSize; z++)
            {
                var noise = (int)(Mathf.PerlinNoise(x * precision, z * precision) * depth);
                var pos = new Vector3(x, noise, z);
                superficie[x, z] = pos;
            }
        }

        foreach(Vector3 pos in superficie)
        {
            var height = (int)pos.y;

            for(; height >= 0; height --)
            {
                var obj = Instantiate(GetObjectByLayerList(height));
                obj.transform.position = new Vector3(pos.x, height, pos.z);
            }
        }
    }
}