using UnityEngine;
using System.Collections.Generic;

public class FreezeTurret : Turret {
	
    private float speedMultiplier = 0.10f;
	private float alteredSpeedDuration = 2.0f;

	private ParticleSystem particles;
	private float particlesStartTime;

    protected override void Awake() {
        base.Awake();
        damage = 5;
        particles = GetComponentInChildren<ParticleSystem>();
    }

	protected override void Update() {
		Fire();
	}

    protected override void Fire() {
        if (TargetEnemy()) {
			if (Time.time - particlesStartTime > particles.duration) {
				foreach (GameObject e in enemies) {
					e.GetComponent<Enemy>().TakeDamage(damage);
					e.GetComponent<Enemy>().AlterSpeed(speedMultiplier, alteredSpeedDuration);
					if (!particles.isPlaying) {
						particles.Play ();
					}
					particlesStartTime = Time.time;
				}
            }
        } else {
			if (Mathf.Approximately(Mathf.Round(Time.time - particlesStartTime) / particles.duration, 1)) {
				particles.Stop ();
			}
        }
    }
}