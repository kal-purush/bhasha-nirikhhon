using System.Collections;
using Unity.EditorCoroutines.Editor;
using UnityEngine;

public class InEditorRotator : MonoBehaviour
{

    public float timeInterval = 0.03f;
    public float speed = 3f;

    public bool active = false;

    private bool stop = false;


    static private EditorCoroutine rotationCoroutine;
    // Start is called once before the first execution of Update after the MonoBehaviour is created
    void OnValidate()
    {
        if (rotationCoroutine != null){
            stop = true;
            return;
        }
        transform.localRotation = Quaternion.identity;
        if(!active){
            return;
        }
        rotationCoroutine = EditorCoroutineUtility.StartCoroutine(Rotate(), this);
    }

    IEnumerator Rotate(){
        while (active && !stop){
            transform.rotation *= Quaternion.Euler(0f, 90f * speed * timeInterval,0f);
            yield return new WaitForSecondsRealtime(timeInterval);
        }
        stop = false;
        rotationCoroutine = null;
    }
}