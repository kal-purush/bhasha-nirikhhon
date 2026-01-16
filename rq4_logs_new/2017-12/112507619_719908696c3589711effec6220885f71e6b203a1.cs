using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ObjectSizeChange : MonoBehaviour {

    private Color color;
    private SpriteRenderer sr;
    public TextMesh tm;
    private bool destroy;
    private bool startPressing;

	// Use this for initialization
	void Start () {
        sr = GetComponent<SpriteRenderer>();
        color = sr.color;
        destroy = true;
        startPressing = false;
    }
	
	// Update is called once per frame
	void Update () {
        StartCoroutine(ChangeSizeAndBrightness());
        if (startPressing)
        {
            if (Input.GetKeyDown(KeyCode.A))
            {
                destroy = false;
            }
        }
	}

    IEnumerator ChangeSizeAndBrightness()
    {
        yield return new WaitForSeconds(2);
        startPressing = true;
        color.a = 1.0f;
        sr.color = color;
        transform.localScale = new Vector3(3.5f, 3.5f, 1.0f);
        StartCoroutine(DestroyPlayer());
    }

    IEnumerator DestroyPlayer()
    {
        yield return new WaitForSeconds(1);
        if (destroy)
        {
            Destroy(gameObject);
        }
    }
}