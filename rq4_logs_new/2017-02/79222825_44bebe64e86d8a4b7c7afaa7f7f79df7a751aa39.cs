using UnityEngine;
using System.Collections;

public class EnemyMovement : MonoBehaviour
{
	public Vector3 relative_target = Vector3.zero;
	public Vector3[] relative_targets;
    public float move_speed = 2f;
	public int index = 0;
    private Vector3 original_pos;
    private GameObject player;

    void Start()
    {
        original_pos = transform.position;
		relative_target = transform.position;
        player = GameObject.Find("Player");
    }

    void Update()
    {
		if (Vector2.Distance (transform.position, relative_target) > 0.01f)
			transform.position = Vector2.MoveTowards(transform.position, relative_target, move_speed * Time.deltaTime);
		else {
			cycleIndex ();
		}

    }

   
	void cycleIndex()
	{
		if (index >= relative_targets.Length) {
			index = 0;
			relative_target = original_pos;
		} else {
			relative_target = transform.position + relative_targets [index];
		}
		index++;
	}
}