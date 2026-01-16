using UnityEngine;
using System.IO;
using System.Collections.Generic;

public class HumanModel : MonoBehaviour
{
    public Transform keypointPrefab;
    public GameObject cubePrefab; // Prefab for the cubes representing connections
    public string jsonFilePath = "/home/Downloads/movementShuaiID34587.txt"; // Change this path accordingly

    private List<Transform> keypoints = new List<Transform>();
    private List<GameObject> cubes = new List<GameObject>();

    [System.Serializable]
    public class KeypointData
    {
        public float x;
        public float y;
        public float z;
    }

    [System.Serializable]
    public class FrameData
    {
        public List<KeypointData> features;
    }

    [System.Serializable]
    public class KeypointsFrame
    {
        public int frame_id;
        public List<FrameData> objects;
    }

    void Start()
    {
        LoadDataFromFile(jsonFilePath);
    }

    void LoadDataFromFile(string filePath)
    {
        if (File.Exists(filePath))
        {
            string jsonContent = File.ReadAllText(filePath);
            KeypointsFrame keypointData = JsonUtility.FromJson<KeypointsFrame>(jsonContent);

            CreateKeypoints(keypointData);
            CreateCubes();
        }
        else
        {
            Debug.LogError("File not found: " + filePath);
        }
    }

    void CreateKeypoints(KeypointsFrame keypointData)
    {
        foreach (var frameData in keypointData.objects)
        {
            foreach (var keypoint in frameData.features)
            {
                Transform newKeypoint = Instantiate(keypointPrefab, new Vector3(keypoint.x, keypoint.y, keypoint.z), Quaternion.identity);
                keypoints.Add(newKeypoint);
            }
        }
    }

    void CreateCubes()
    {
        for (int i = 0; i < keypoints.Count - 1; i++)
        {
            Vector3 cubePosition = (keypoints[i].position + keypoints[i + 1].position) / 2;
            GameObject newCube = Instantiate(cubePrefab, cubePosition, Quaternion.identity);
            newCube.transform.localScale = new Vector3(Vector3.Distance(keypoints[i].position, keypoints[i + 1].position), 0.1f, 0.1f);
            newCube.transform.LookAt(keypoints[i + 1].position);
            cubes.Add(newCube);
        }
    }

    void Update()
    {
        // Update the positions of keypoints
        for (int i = 0; i < keypoints.Count - 1; i++)
        {
            cubes[i].transform.position = (keypoints[i].position + keypoints[i + 1].position) / 2;
            cubes[i].transform.localScale = new Vector3(Vector3.Distance(keypoints[i].position, keypoints[i + 1].position), 0.1f, 0.1f);
            cubes[i].transform.LookAt(keypoints[i + 1].position);
        }
    }
}
