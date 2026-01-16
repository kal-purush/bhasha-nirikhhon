using UnityEngine;
using System.Collections;

public class NewBehaviourScript : MonoBehaviour {
    [SerializeField]
    private Vector3 cube_pos;
    [SerializeField]
    private float size;

    float cube_size;
	// Use this for initialization
	void Start () {
        cube_size = size;
        transform.localPosition = new Vector3(cube_pos.x * cube_size, cube_pos.y * cube_size, cube_pos.z * cube_size);
        transform.localScale = new Vector3(cube_size, cube_size, cube_size);

    }

    // Update is called once per frame
    void Update () {
        transform.Rotate(1, 0, 1);
        cube_size -= 0.003f;
        transform.localPosition = new Vector3(cube_pos.x * cube_size, cube_pos.y * cube_size, cube_pos.z * cube_size);
        transform.localScale = new Vector3(cube_size, cube_size, cube_size);

        if (cube_size <= 0.0f) {
            cube_size =  1.2f;
        }
    }
}