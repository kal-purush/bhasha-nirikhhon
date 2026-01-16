using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LerpBack : MonoBehaviour
{
	[SerializeField]
	private float smooth = 10.0f;
	Quaternion target_rot;
	[SerializeField]
	Vector3 target_pos;
    // Start is called before the first frame update
    void Start()
    {
		target_rot = Quaternion.Euler(0, 0, 0);
		target_pos = transform.localPosition;
    }

    // Update is called once per frame
    void Update()
    {
        transform.rotation = Quaternion.Slerp(transform.rotation, target_rot,  Time.deltaTime * smooth);
		transform.localPosition = Vector3.Lerp(transform.localPosition, target_pos, Time.deltaTime * smooth);
		//transform.speed = 1;
    }
}