using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BarrierBehaviour : MonoBehaviour {
    public float deltaTime; // 相邻障碍物间隔时间
    private float dequeueTime = 0.01f; // 队头出队时间
    public Transform[] members; // 队列元素
    private Vector3 recordTrans;
    private Queue<Transform> barrierQueue; // 将障碍物保存至一个队列中
    private List<Transform> headList; // 队头组成的列表
    private Transform queueHead; // 队头

	// Use this for initialization
	void Start () {
        headList = new List<Transform>();
        barrierQueue = new Queue<Transform>(members); // 将空父物体的所有子物体保存至队列
        recordTrans = members[0].position;
	}
	
	// Update is called once per frame
	void Update () {
        // 当前时间减去上一次出队时间如果大于间隔时间则出队
        if (Time.time - dequeueTime >= deltaTime)
        {
            queueHead = barrierQueue.Dequeue();
            headList.Add(queueHead);
            queueHead.GetComponent<BarrierMovement>().isReady = true;
            dequeueTime = Time.time;
            deltaTime = Random.Range(1f, 6f); // 随机间隔时间1.5~3.5
        }

        // 障碍物离开屏幕后将其位置还原并加入到队尾
        if (queueHead != null)
        {
            for (int i = 0; i < headList.Count; i++ )
            {
                if (headList[i].position.y <= -4.5)
                {
                    headList[i].GetComponent<BarrierMovement>().isReady = false;
                    headList[i].position = recordTrans;
                    barrierQueue.Enqueue(headList[i]);
                    headList.Remove(headList[i]);
                }
            }
        }
	}
}