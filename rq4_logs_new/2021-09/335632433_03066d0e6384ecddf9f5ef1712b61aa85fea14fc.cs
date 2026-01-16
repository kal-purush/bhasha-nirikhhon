using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class EnemyPunch : MonoBehaviour
{
    // Start is called before the first frame update
    public Transform target;
    public GameObject donguri;
    NavMeshAgent agent;
    NavMeshObstacle obstacle;
    public float Speed;
    public float rotateSpeed;
    public float stopDist;
    public float searchDist;

    private Animator animator;
    private string walkStr = "isWalk";
    private string panchStr = "isPunch";

    bool setVec = false;
    void Start()
    {
        agent = GetComponent<NavMeshAgent>();
        obstacle = GetComponent<NavMeshObstacle>();
        animator = donguri.GetComponent<Animator>();
        GameObject targetObject = target.gameObject;
    }

    // Update is called once per frame
    void Update()
    {
        bool isAttack = animator.GetCurrentAnimatorStateInfo(0).IsName("Base Layer.Punch");
        bool isWait = animator.GetCurrentAnimatorStateInfo(0).IsName("Base Layer.Wait");

        if (Vector3.Distance(agent.transform.position, target.position) <= searchDist)
        {
            if (target != null && agent.enabled == true)
            {
                agent.destination = target.position;
                agent.speed = Speed;
                /*経路探索を終了するstoppingDistanceとアニメーションを遷移させるstopDistが同じ値だと
                　不具合があったので、-0.3fした距離を設定*/
                agent.stoppingDistance = stopDist - 0.3f;
            }

            if (Vector3.Distance(agent.transform.position, target.position) <= stopDist)
            {
                agent.enabled = false;
                //obstacle.enabled = true;

                //Waitモーション時にプレイヤーの方を向かせる
                if (isWait == true)
                {
                    float speed = 0.03f;
                    Vector3 vec = target.position - transform.position;
                    Vector3 nvec = new Vector3(vec.x, transform.position.y, vec.z);
                    Quaternion rotation = Quaternion.LookRotation(nvec);
                    transform.rotation = Quaternion.Slerp(this.transform.rotation, rotation, speed);
                    //transform.LookAt(nvec);
                    Vector3 enemyVec = transform.eulerAngles;
                    enemyVec.x = 0.0f;
                    enemyVec.z = 0.0f;

                    transform.eulerAngles = enemyVec;
                    setVec = true;
                }

                this.animator.SetBool(walkStr, false);
                this.animator.SetBool(panchStr, true);
            }
            else
            {
                this.animator.SetBool(panchStr, false);

                if (isAttack == false)
                {

                    agent.enabled = true;
                    //obstacle.enabled = false;

                    this.animator.SetBool(walkStr, true);
                }
            }
        }
        else
        {
            this.agent.enabled = false;
            this.animator.SetBool(walkStr, false);
        }
    }
}