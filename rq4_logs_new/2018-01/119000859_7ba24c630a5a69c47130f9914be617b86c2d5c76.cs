using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(ShooterProjectile))]
public class EnemyShooting : MonoBehaviour {
    public float shootIntervalSecond = 2;

    private float timeElapsed = 0.0f;
    private ShooterProjectile shooter;
	
    void Awake()
    {
        shooter = GetComponent<ShooterProjectile>();
    }

	// Update is called once per frame
	void Update () {
        timeElapsed += Time.deltaTime;

        if (timeElapsed >= shootIntervalSecond)
        {
            shooter.Shoot();
            timeElapsed = 0.0f;
        }
	}
}