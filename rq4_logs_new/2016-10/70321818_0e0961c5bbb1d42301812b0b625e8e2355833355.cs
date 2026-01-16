using UnityEngine;
using System.Collections;

public class touch : MonoBehaviour
{

    int cout;
    // Use this for initialization
    void Start()
    {
        cout = 0;
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Vector2 tapPoint = Camera.main.ScreenToWorldPoint(Input.mousePosition);
            cout++;
            Collider2D collider = Physics2D.OverlapPoint(tapPoint);
            if (collider != null)
            {
                GameObject obj = collider.transform.gameObject;
                if (cout == 3) {
                    Destroy(obj);
                }
            }



        }
    }
}