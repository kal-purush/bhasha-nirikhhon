using UnityEngine;
using System.Collections;

public class ScaleEffecter : MonoBehaviour
{
    [SerializeField]
    private float targetScale = 1.2f;

    private bool biggen = true;

	// Use this for initialization
	void Start ()
    {
	    if(targetScale < 1f)
        {
            Debug.LogWarning("INVALID Value");
            enabled = false;
        }
	}
	
	// Update is called once per frame
	void Update ()
    {
        float speed = 0.2f;
	    if(!GameManager.Instance.IsPause)
        {
            Vector3 scale = transform.localScale;
            float val = speed * Time.smoothDeltaTime;

            if(!biggen)
            {
                val *= -1;
            }

            scale.x += val;
            scale.y += val;

            transform.localScale = scale;

            if (scale.x < 1.0f)
            {
                scale.x = 1;
                scale.y = 1;

                transform.localScale = scale;
                biggen = true;
            }
            else if (scale.x > targetScale)
            {
                scale.x = targetScale;
                scale.y = targetScale;

                transform.localScale = scale;
                biggen = false;
            }
        }
	}
}