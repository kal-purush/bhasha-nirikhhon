using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PathManager : MonoBehaviour
{
    public static PathManager Instance;
    [SerializeField]
    private List<Transform> _foodPath;
    public void Start()
    {
        if (Instance == null)
        {
            Instance = new();
            Instance = this;
        }
        else
            Destroy(gameObject);
    }
    // Update is called once per frame
    void Update()
    {
        
    }
    public (bool, int) NextCheckpoint(int currentIndex, bool isReturningHome, out Vector3 nextCheckpoint)
    {
        int nextIndex = currentIndex;
        if (!isReturningHome)
        {
            nextIndex--;
            if (nextIndex < 0)
            {
                nextIndex = 1;
                isReturningHome = true;
            }
        }
        else
        {
            nextIndex++;
            if (nextIndex >= _foodPath.Count)
            {
                nextIndex = 0;
                isReturningHome = false;
            }
        }
        Vector3 position  = _foodPath[nextIndex].position;
        nextCheckpoint = position;
        return (isReturningHome, nextIndex);
    }
}