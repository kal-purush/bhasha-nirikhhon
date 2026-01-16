using UnityEngine;
using System.Collections;

public class linerenderellipse : MonoBehaviour {

    public int segments;
    public float xradius;
    public float yradius;
    LineRenderer line;

    private float nextActionTime = 0.0f;
    public float period = 0.1f;

    private float xTemp;
    private float yTemp;
    
    private bool reverse;

    void Start() {
        line = gameObject.GetComponent<LineRenderer>();
        line.material = new Material(Shader.Find("Particles/AdditiveCustom")); 
        line.SetVertexCount(segments + 1);
        line.useWorldSpace = false;
        CreatePoints();
        xTemp = xradius;
        yTemp = 1;
    }


    void CreatePoints() {
        float x;
        float y;
        float z = 0f;

        float angle = 20f;

        for (int i = 0; i < (segments + 1); i++) {
            x = Mathf.Sin(Mathf.Deg2Rad * angle) * xradius;
            y = Mathf.Cos(Mathf.Deg2Rad * angle) * 0;

            line.SetPosition(i, new Vector3(x, y, z));

            angle += (360f / segments);
        }
    }

    void Update() {
        if (Time.time > nextActionTime) {
            nextActionTime = Time.time + period;
            if (xTemp > xradius || yTemp > yradius) {
                reverse = true;
            }
            else if (xTemp < 0 || yTemp < 0) {
                reverse = false;
            }
            if (!reverse) {
                xTemp = xTemp + Random.Range(0.1f,0.6f);
                yTemp = yTemp + Random.Range(0.1f, 0.6f);
            }
            else {
                xTemp = xTemp - Random.Range(0.1f, 0.6f);
                yTemp = yTemp - Random.Range(0.1f, 0.6f);
            }

            float x;
            float y;
            float z = 0f;

            float angle = 20f;
            for (int i = 0; i < (segments + 1); i++) {
                x = Mathf.Sin(Mathf.Deg2Rad * angle) * xTemp;
                y = Mathf.Cos(Mathf.Deg2Rad * angle) * yTemp;

                line.SetPosition(i, new Vector3(x, y, z));

                angle += (360f / segments);
            }
        }
    }

    void OnDrawGizmos() {
        Gizmos.color = Color.yellow;
        Gizmos.DrawSphere(transform.position, 1);
    }
}