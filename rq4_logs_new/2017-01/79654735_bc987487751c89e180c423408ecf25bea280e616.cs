using UnityEngine;
using System.Collections;

public class CursorScript : MonoBehaviour {

    public float cursorSpeed;
    public Boundary boundary;

    public GameObject wavePrefab;

    // Use this for initialization
    void Start ()
    {
	
	}

    [System.Serializable]
    public class Boundary
    {
        public float xMin, xMax, yMin, yMax;
    }

    // Update is called once per frame
    void Update ()
    {
        float moveHorizontal = Input.GetAxis("Horizontal") * Time.deltaTime * cursorSpeed;
        float moveVertical = Input.GetAxis("Vertical") * Time.deltaTime * cursorSpeed;

        Vector3 movement = new Vector3(moveHorizontal, moveVertical, 0.0f);

        gameObject.transform.Translate(movement);

        gameObject.transform.position = new Vector3
        (
            Mathf.Clamp(gameObject.transform.position.x, boundary.xMin, boundary.xMax),
            Mathf.Clamp(gameObject.transform.position.y, boundary.yMin, boundary.yMax),
            0.0f
        );

        if(Input.GetButtonDown("Fire2"))
        {
            Fire();
        }
    }

    void Fire()
    {
        Instantiate(wavePrefab, gameObject.transform.position, Quaternion.identity);
    }
}