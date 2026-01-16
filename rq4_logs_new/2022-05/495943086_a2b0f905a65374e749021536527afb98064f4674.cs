using UnityEngine;

public class ExampleMonoBehaviour1 : MonoBehaviour
{
    private void Awake()
    {
        Debug.Log("[ExampleMonoBehaviour1] Awake");
    }

    private void Start()
    {
        Debug.Log("[ExampleMonoBehaviour1] Start shouldn't be called!");
    }
}

public class ExampleMonoBehaviour2 : MonoBehaviour
{
    private void Awake()
    {
        Debug.Log("[ExampleMonoBehaviour2] Awake");
    }

    private void Start()
    {
        Debug.Log("[ExampleMonoBehaviour2] Start shouldn't be called!");
    }
}