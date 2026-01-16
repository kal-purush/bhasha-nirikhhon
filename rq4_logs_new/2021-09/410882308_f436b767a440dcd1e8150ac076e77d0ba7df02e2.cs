using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CubeBehaviours : EnemyBehaviour
{
    [SerializeField] private Rigidbody2D laser;
    [SerializeField] private float laserSpeed;
    private Color color;
    [SerializeField] private Animation glow;
    [SerializeField] private string animationName;

    public override void Update()
    {
        if (resetShoot && WaveManager.cubeShooting == false)
        {
            StartCoroutine(Shooting());
        }

        if (transform.position.y <= 2.5f)
        {
            self.velocity = Vector2.zero;
            
        }
        if (life <= 0)
        {
            GameManager.Instance.AddScore(scoreGive);
            gameObject.SetActive(false);
            WaveManager.cubeShooting = false;
            waveManager = GameObject.Find("WaveManager").transform;
            WaveManager.enemiesLeft = WaveManager.enemiesLeft - 1;
        }
        
        IEnumerator Shooting()
        {
            resetShoot = false;
            WaveManager.cubeShooting = true;
            glow.Play(animationName);
            yield return new WaitForSeconds(1.5f);
            Debug.Log("start corout");
            Rigidbody2D shotLaser = Instantiate(laser, new Vector2(0f, 37f), Quaternion.identity);
            shotLaser.AddForce(Vector3.down * laserSpeed);
            Invoke(("ResetShoot"), shootingRate);
            StartCoroutine(CubeShootingReset());
        }
        
        IEnumerator CubeShootingReset()
        {
            yield return new WaitForSeconds(4f);
            WaveManager.cubeShooting = false;
        }
        
    }
}