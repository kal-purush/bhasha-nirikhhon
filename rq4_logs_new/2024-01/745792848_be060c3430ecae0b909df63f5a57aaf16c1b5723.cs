using UnityEngine;
using UnityEngine.Serialization;

public class FoliageGenerator : MonoBehaviour
{
    [Header("Dependencies")]
    [SerializeField] private CameraVariable camVar;
    [SerializeField] private GameObject foliagePrefab;

    [Header("Parameters")]
    [SerializeField] private float foliageSpacing = 5;
    
    [Header("State")]
    [SerializeField] private int leftBound = 0;
    [SerializeField] private int rightBound = 1;

    private void Update()
    {
        if (camVar.value != null)
        {
            float diff = camVar.value.transform.position.x - transform.position.x;
            int left = Mathf.FloorToInt((diff - (camVar.value.orthographicSize * Screen.width / Screen.height) * 1.5f) / foliageSpacing);
            int right = Mathf.FloorToInt((diff + (camVar.value.orthographicSize * Screen.width / Screen.height) * 1.5f) / foliageSpacing);
            while (leftBound >= left)
            {
                GameObject tree = Instantiate(foliagePrefab, transform);
                tree.transform.localPosition = Vector3.right * (leftBound * foliageSpacing);
                leftBound--;
            }

            while (rightBound <= right)
            {
                GameObject tree = Instantiate(foliagePrefab, transform);
                tree.transform.localPosition = Vector3.right * (rightBound * foliageSpacing);
                rightBound++;
            }
        }
    }
}