using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ShootProjectile2 : MonoBehaviour
{

    public Image reticleImage;
    public Color reticleEnemyColor;

    Color originalReticleColor;

    public Weapon currentWeapon;

    // Start is called before the first frame update
    void Start()
    {
        originalReticleColor = reticleImage.color;

        currentWeapon = GetComponentInChildren<Weapon>();
    }

    // Update is called once per frame
    void Update()
    {
        if (PlayerLookController.locked || PlayerMovementController.locked) return;

        if (Input.GetButtonDown("Fire1"))
        {
            if (currentWeapon != null)
            {
                currentWeapon.Fire(transform.position, transform.rotation);
            }
        }
    }

    private void FixedUpdate()
    {
        ReticleEffect();
    }

    void ReticleEffect()
    {
        RaycastHit hit;

        if (Physics.Raycast(transform.position, transform.forward, out hit, Mathf.Infinity))
        {
            if (hit.collider.CompareTag("Enemy"))
            {
                reticleImage.color = Color.Lerp
                    (reticleImage.color, reticleEnemyColor, Time.deltaTime * 5);
            }
            else
            {
                reticleImage.color = Color.Lerp
                    (reticleImage.color, originalReticleColor, Time.deltaTime * 5);
            }
        }
    }
}