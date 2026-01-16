
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerSizeChange : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown("q"))
        {
            if (transform.localScale.x != 8f)
            {
                gameObject.transform.localScale = new Vector3(
                    transform.localScale.x * 2f, transform.localScale.y * 2f, transform.localScale.z * 2f);
            }
        }
        if (Input.GetKeyDown("e"))
        {
            if (transform.localScale.x != 2f)
            {
                gameObject.transform.localScale = new Vector3(
                    transform.localScale.x / 2f, transform.localScale.y / 2f, transform.localScale.z / 2f);
            }
        }
    }
}