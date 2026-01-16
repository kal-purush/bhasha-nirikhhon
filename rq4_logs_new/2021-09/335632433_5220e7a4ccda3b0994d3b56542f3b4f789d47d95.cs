using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BossAttackHit : MonoBehaviour
{
    public GameObject player;
    public GameObject leftFootColl;
    public GameObject rightFootColl;
    public GameObject breathObj;
    public float stumpHitradius;
    public bool hitBreath = false;
    private bool isGround = false;
    public bool hitLeftStump = false;
    public bool hitRightStump = false;
    PlayerMove playerScript;
    BossDonguriBreathHit bossBreathHit;
    BossBreathEvent bossBreathEvent;
    BossDonguriAnim bossStumpEvent;

    // Start is called before the first frame update
    void Start()
    {
        bossBreathEvent = GetComponent<BossBreathEvent>();
        bossStumpEvent = GetComponent<BossDonguriAnim>();
        bossBreathHit = breathObj.GetComponent<BossDonguriBreathHit>();
        playerScript = player.GetComponent<PlayerMove>();
    }

    // Update is called once per frame
    void Update()
    {
        //ブレス攻撃が当たったかどうか
        if (bossBreathEvent.IsBreath && bossBreathHit.isBreathHit)
        {
            hitBreath = true;
        }
        else
        {
            hitBreath = false;
        }

        //プレイヤーが地面にいるかどうか
        if (playerScript.playerState == playerScript.groundState)
        {
            isGround = true;
            
        }
        else
        {
            isGround = false;
        }

        if (isGround)
        {
            //足踏み攻撃が当たったかどうか
            if (bossStumpEvent.isLeftFoot)
            {
                hitLeftStump = stumpHit(player, leftFootColl, rightFootColl, stumpHitradius, 1);
            }
            else
            {
                hitLeftStump = false;
            }

            if (bossStumpEvent.isRightFoot)
            {
                hitRightStump = stumpHit(player, leftFootColl, rightFootColl, stumpHitradius, 2);
            }
            else
            {
                hitRightStump = false;
            }
        }

    }

    private bool stumpHit(GameObject player,GameObject leftFoot,GameObject rightFoot,float radius,int stumpFlag)
    {
        bool hit = false;
        Vector3 playerPos = player.transform.position;

        if (stumpFlag == 1)
        {
            Vector3 leftFootPos = leftFoot.transform.position;

            float dist = Mathf.Sqrt(Mathf.Pow(playerPos.x - leftFootPos.x, 2) + Mathf.Pow(playerPos.z - leftFootPos.z, 2));

            if (dist <= radius)
            {
                hit = true;
                return hit;
            }
        }

        if (stumpFlag == 2)
        {
            Vector3 rightFootPos = rightFoot.transform.position;

            float dist = Mathf.Sqrt(Mathf.Pow(playerPos.x - rightFootPos.x, 2) + Mathf.Pow(playerPos.z - rightFootPos.z, 2));

            if (dist <= radius)
            {
                hit = true;
                return hit;
            }
        }

        return hit;
    }
}