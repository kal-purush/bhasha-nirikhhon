using System;
using UnityEngine;

public class TouchEffectManager: MonoBehaviour
{
    public static TouchEffectManager Instance { get; private set; }
    private ObjectPooling touchEffectObjectPooling;
    private string nameEffect = "TouchEffect";

    private void Awake()
    {
        Instance = this;
        touchEffectObjectPooling = GetComponentInChildren<ObjectPooling>(); 
        DontDestroyOnLoad(this.gameObject);
    }

    private void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Debug.Log("Touch Effect");
            GameObject touchObject = touchEffectObjectPooling.GetObject(nameEffect);
            touchObject.transform.position = Camera.main.ScreenToWorldPoint(Input.mousePosition);
            touchObject.transform.position = new Vector3(touchObject.transform.position.x, touchObject.transform.position.y, 0);
        }
    }

    public void ReleaseEffect(GameObject effectObj)
    {
        touchEffectObjectPooling.ReleaseObject(effectObj);
    }
}