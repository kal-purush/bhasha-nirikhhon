using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BulletShooting : MonoBehaviour
{
    [SerializeField] Transform shootPoint;
    [SerializeField] GameObject bullet;
    [SerializeField] float bulletSpeed = 10f;

    private float bulletSpeedMax = 30f;

    [Header("****trajectory display****")]
    [SerializeField] LineRenderer lineRenderer;
    [SerializeField] int linePoints = 175;
    [SerializeField] float timeIntervalinPoints = 0.1f;


    private void Update()
    {
        if (Input.GetKey(KeyCode.Space))
        {
            if(bulletSpeed < bulletSpeedMax)
            {
                bulletSpeed += 2 * Time.deltaTime;
            }
        }
        //press space up to shoot
        if (Input.GetKeyUp(KeyCode.Space))
        {
            var bulletfire = Instantiate(bullet, shootPoint.position, shootPoint.rotation);
            bulletfire.GetComponent<Rigidbody2D>().velocity = bulletSpeed * shootPoint.up;
        }

        if(lineRenderer != null)
        {
            DrawTrajectory();
            lineRenderer.enabled = true;
        }
        else
        {
            lineRenderer.enabled = false;
        }

    }

    void DrawTrajectory()
    {
        Vector2 origin = shootPoint.position;
        Vector2 startVelocity = bulletSpeed * shootPoint.up;
        lineRenderer.positionCount = linePoints;

        float time = 0;

        for(int  i = 0; i < linePoints; i++)
        {
            //s = u*t + 1/2*g*t*t
            var x = (startVelocity.x * time) + (Physics2D.gravity.x / 2 * time * time);
            var y = (startVelocity.y * time) + (Physics2D.gravity.y / 2 * time * time);
            Vector2 point = new Vector2(x, y);
            lineRenderer.SetPosition(i, origin + point);
            time+= timeIntervalinPoints;
        }
    }
}