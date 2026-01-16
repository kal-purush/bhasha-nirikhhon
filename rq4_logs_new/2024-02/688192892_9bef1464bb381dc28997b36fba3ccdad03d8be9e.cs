using UnityEngine;
using UnityEngine.InputSystem;
using OmnicatLabs.Timers;

public interface IChargeable
{
    public void OnCharge()
    {
        
    }
}


public class LightningGun : MonoBehaviour
{
    public float size = 1f;
    public float range = 3f;
    public float tickInterval = .2f;
    public GameObject startPoint;

    private bool holding = false;
    private bool onCooldown = false;
    private float elapsedTime = 0f;
    
    public void OnShoot(InputAction.CallbackContext ctx) => holding = ctx.performed ? true : ctx.canceled ? false : false;

    private void FixedUpdate()
    {
        if (holding)
        {
            foreach (RaycastHit hit in Physics.CapsuleCastAll(
                    startPoint.transform.position, 
                    new Vector3(startPoint.transform.position.x, startPoint.transform.position.y, startPoint.transform.position.z * range), 
                    size, transform.forward, 0f))
            {
                if (hit.transform.TryGetComponent(out IChargeable chargeableObj) && !onCooldown)
                {
                    chargeableObj.OnCharge();
                    onCooldown = true;
                }
            }
        }
    }

    private void Update()
    {
        if (onCooldown)
        {
            elapsedTime += Time.deltaTime;
            if (elapsedTime >= tickInterval)
            {
                onCooldown = false;
                elapsedTime = 0f;
            }
        }
    }

    private void OnDrawGizmosSelected()
    {
        Gizmos.color = Color.green;
        Gizmos.DrawWireSphere(startPoint.transform.position, size);
        Gizmos.DrawWireSphere(new Vector3(startPoint.transform.position.x, startPoint.transform.position.y, startPoint.transform.position.z * range), size);
        Gizmos.DrawLine(startPoint.transform.position, new Vector3(startPoint.transform.position.x, startPoint.transform.position.y, startPoint.transform.position.z * range));
    }
}