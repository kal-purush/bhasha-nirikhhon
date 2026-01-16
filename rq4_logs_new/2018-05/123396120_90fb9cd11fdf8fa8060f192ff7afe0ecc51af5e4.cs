using UnityEngine;
using System.Collections;

public class Follow : MonoBehaviour
{

    // 路径脚本
    [SerializeField]
    private WaypointCircuit circuit;

    //移动距离
    private float dis;
    //移动速度
    public float speed = 1;

    void Update()
    {
        //计算距离
        dis += Time.deltaTime * speed;
        //获取相应距离在路径上的位置坐标
        transform.position = circuit.GetRoutePoint(dis).position;
        //获取相应距离在路径上的方向
        transform.rotation = Quaternion.LookRotation(circuit.GetRoutePoint(dis).direction);
    }
}