using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HealStunComboHandler : HealStunHandler {

	private bool passThrough = false;

	protected override bool StunAbsorb()
    {
    	if(state == State.NULL)
    		state = State.STUNABSORB;
        
        if(state == State.STUNABSORB)
        {
            if(stun == null)
            {
                stun = Instantiate(stunPrefab, transform.position + (transform.up * 10), Quaternion.identity);
            }
            else
            {
                stun.transform.localScale += new Vector3(12.0f, 12.0f, 0f);
                if(stun.GetComponent<StunProjectile>().AddAmount(10) >= 40)
                {
                	PassThrough();
                	stun.GetComponent<StunProjectile>().fired = true;
                    stun.GetComponent<StunProjectile>().DestroySoon();

                }
            }
            return true;
        }
        return false;
    }

    protected override bool HealAbsorb()
    {
    	if(state == State.NULL)
    		state = State.HEALABSORB;
        
        if(state == State.HEALABSORB)
        {
            if(heal == null)
            {
                heal = Instantiate(healPrefab, transform.position + -(transform.up * 10), Quaternion.identity);
            }
            else
            {
            	//TODO: Change this to use a getter then a setter to remove a tiny bug
                if(heal.GetComponent<HealPickup>().GetHealAmount() <= 40)
                {
                	heal.GetComponent<HealPickup>().AddHealAmount(8);
                	heal.transform.localScale += new Vector3(12.0f, 12.0f, 0f);
                }
                else
                {
                	PassThrough();
                }
            }
            return true;
        }
        return false;
    }

    private void PassThrough()
    {
		gameObject.GetComponent<Collider2D>().enabled = false;
    }

}