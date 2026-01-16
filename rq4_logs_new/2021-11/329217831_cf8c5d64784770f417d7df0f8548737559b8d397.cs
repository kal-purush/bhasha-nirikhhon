using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraFollow : Singleton<CameraFollow>
{
    GameObject curStageFollowTarget;
    public override void Init()
    {

    }

    private void Update()
    {
        transform.position = new Vector3(curStageFollowTarget.transform.position.x, curStageFollowTarget.transform.position.y, -10);
        
    }

    public void ChangeFollowTarget(GameObject followTarget)
    {
        curStageFollowTarget = followTarget;
    }
}