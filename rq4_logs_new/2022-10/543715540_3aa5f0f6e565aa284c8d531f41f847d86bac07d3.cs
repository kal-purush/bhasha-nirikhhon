using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bow : MonoBehaviour
{
    public float damage = 5f;
    //public float range = 100f;

    private bool equippedBow = true;
    private bool canShoot = true;
    [SerializeField] private float shootCooldownTime = 0.45f;

    public float shootForce, upwardForce;

    public Camera cam;
    public Transform attackPoint;
    [SerializeField] private GameObject arrow;

    private void Update()
    {
        // Shoot
        if (Input.GetMouseButtonDown(0) && equippedBow && canShoot)
        {
            Vector3 targetPoint;
            Ray ray = cam.ViewportPointToRay(new Vector3(0.5f, 0.5f, 0));
            canShoot = false;
            RaycastHit hit;
            if(Physics.Raycast(cam.transform.position, cam.transform.forward, out hit))
            {
                targetPoint = hit.point;
            }
            else
            {
                targetPoint = ray.GetPoint(75);
            }

            Vector3 directionWithoutSpread = targetPoint - attackPoint.position;

            GameObject currentArrow = Instantiate(arrow, attackPoint.position, Quaternion.identity);
            currentArrow.transform.forward = directionWithoutSpread.normalized;
            currentArrow.GetComponent<Rigidbody>().AddForce(directionWithoutSpread.normalized * shootForce, ForceMode.Impulse);
            currentArrow.GetComponent<Rigidbody>().AddForce(cam.transform.up * upwardForce, ForceMode.Impulse);

            //animator.SetTrigger("Attack");
            Invoke(nameof(ResetShoot), shootCooldownTime);
        }
    }

    void ResetShoot()
    {
        canShoot = true;
    }
}