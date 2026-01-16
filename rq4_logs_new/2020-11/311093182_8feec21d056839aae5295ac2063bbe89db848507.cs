using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CrowdMember : MonoBehaviour
{
    //const float CROWD_SIZE = 0.25f;
    const float CROWD_SIZE_VARI = 0.05f;
    //Vector3 CROWD_POS = new Vector3(0, 0.2f, -3);
    const float CROWD_POS_OFFSET = 1.5f;
    const float CROWD_POS_VARI = 0.5f;
    const float CROWD_POS_Y = 0.15f;
    const float CROWD_BOB = 0.05f;

    const float COLOUR_LO = 0.1f;
    const float COLOUR_HI = 0.3f;

    // Start is called before the first frame update
    void Start()
    {
        // POSITION
        Vector3 pos = transform.position;
        bool right = Random.value < 0.5f;
        float offset = right ? CROWD_POS_OFFSET : -CROWD_POS_OFFSET;
        offset += Random.Range(-CROWD_POS_VARI, CROWD_POS_VARI);
        pos.x = offset;
        transform.position = pos;

        // SCALE
        Vector3 scale = transform.localScale;
        float scale_ex = Random.Range(0, CROWD_SIZE_VARI);
        scale.x += scale_ex;
        scale.y += scale_ex;
        transform.localScale = scale;

        // COLOUR
        float r = Random.Range(COLOUR_LO, COLOUR_HI);
        float g = Random.Range(COLOUR_LO, COLOUR_HI);
        float b = Random.Range(COLOUR_LO, COLOUR_HI);
        GetComponent<SpriteRenderer>().color = new Color(r, g, b);
    }

    // Update is called once per frame
    void Update()
    {
        float y = CROWD_POS_Y;
        y += CROWD_BOB * Mathf.Sin(Time.time);

        Vector3 pos = transform.position;
        pos.y = y;
        transform.position = pos;
    }
}