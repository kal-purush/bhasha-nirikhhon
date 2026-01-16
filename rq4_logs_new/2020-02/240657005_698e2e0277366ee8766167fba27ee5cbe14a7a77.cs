using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using ZLMsg;

public class AreaController : MonoBehaviour, IMsgReceiver, IMsgSender
{
    public LineRenderer m_LineRenderer;
    private static List<Vector3> areaPosList;

    private void Start()
    {
        this.RegisterLogicMsg(MsgName.MSG_SET_AREA, SetArea);
        areaPosList = new List<Vector3>();
    }

    public void ResetArea()
    {
        areaPosList?.Clear();
        m_LineRenderer.positionCount = 0;
        this.SendLogicMsg(MsgName.MSG_JUDGE_PLAYER_IN_INSIDE, null);
    }

    /// <summary>
    /// 设置范围
    /// </summary>
    /// <param name="obj"></param>
    private void SetArea(IMsgParam obj)
    {
        MsgParam<Vector3> msgParam = obj as MsgParam<Vector3>;
        if(areaPosList != null)
        {
            areaPosList.Add(msgParam.param);
        }
        m_LineRenderer.positionCount = areaPosList.Count;
        m_LineRenderer.SetPositions(areaPosList.ToArray());

        this.SendLogicMsg(MsgName.MSG_JUDGE_PLAYER_IN_INSIDE, null);
    }

    public static bool IsPointInsidePolygon(Vector3 point)
    {
        if (areaPosList != null)
        {
            return IsPointInsidePolygon(point, areaPosList);
        }
        return false;
    }

    /// <summary>
    /// ray-crossing算法,判断一个点是否在一个多边形范围内
    /// </summary>
    /// <param name="point"></param>
    /// <param name="v"></param>
    /// <returns></returns>
    public static bool IsPointInsidePolygon(Vector3 point, List<Vector3> posList)
    {
        int nCross = 0;
        int arrayLen = posList.Count;
        for (int i = 0; i < arrayLen; i++)
        {
            Vector3 v1 = posList[i]; //当前顶点
            Vector3 v2 = posList[(i + 1) % arrayLen]; //下一顶点

            if (v1.z == v2.z) continue; //水平线直接跳过

            if (point.z < Mathf.Min(v1.z, v2.z)) continue;  //目标点低于这个线,跳过
            if (point.z > Mathf.Max(v1.z, v2.z)) continue;  //目标点高于这个线,跳过

            float x = (point.z - v1.z) * (v2.x - v1.x) / (v2.z - v1.z) + v1.x;  //利用相似三角形原理,求出相交点的 x 坐标
            if (x > point.x) //如果交点是在目标点的右边,统计加 1(只向右边)
            {
                nCross++;
            }
        }

        return nCross % 2 == 1; //如果是奇数,在多边形内,否则在外
    }
}