using UnityEngine;
using UnityEngine.Networking;
using System.Collections;
using System.Collections.Generic;


public class SyncTransform : NetworkBehaviour {

    [SerializeField]
    private bool useHistoricalPosLerping = false;

    [SerializeField]
    private bool useHistoricalRotLerping = false;

    [SerializeField]
    Transform myTransform;

    [SyncVar(hook = "SyncPositionValues")]
    private Vector3 pos;
    [SyncVar(hook = "SyncRotationValues")]
    private Vector3 rot;

    private Vector3 lastPos, lastRot;
    

    private float wayPointCloseEnoughDist = 0.11f;
    private float wayPointCloseEnoughRot = 0.4f;

    private float transmissionDistThreshold = 0.2f;
    private float transmissionRotThreshold = 0.3f;

    private float posLerpRate = 15;
    private float fastPosLerpRate = 25;
    private float normalPosLerpRate = 15;
    private float rotLerpRate = 30;

    private List<Vector3> syncPosList = new List<Vector3>();
    private List<Vector3> syncRotList = new List<Vector3>();


    // Use this for initialization
    void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        LerpRotation();
        LerpPosition();
    }

    void FixedUpdate()
    {
        TransmitRotation();
        TransmitPosition();
    }

    void LerpRotation()
    {
        if (!isLocalPlayer)
        {
            if (useHistoricalRotLerping)
            {
                doHistoricalRotInterpolation();
            }
            else
            {
                doOrdinaryRotInterpolation();
            }
        }
    }

    void doHistoricalRotInterpolation()
    {
        if (syncRotList.Count > 0)
        {
            LerpRotation(syncRotList[0]);

            if (Mathf.Abs(myTransform.localEulerAngles.x - syncRotList[0].x) < wayPointCloseEnoughRot &&
                Mathf.Abs(myTransform.localEulerAngles.y - syncRotList[0].y) < wayPointCloseEnoughRot &&
                Mathf.Abs(myTransform.localEulerAngles.z - syncRotList[0].z) < wayPointCloseEnoughRot)
            {
                syncRotList.RemoveAt(0);
            }

            //Debug.Log(syncRotList.Count.ToString() + " syncRotList Count");
        }
    }

    void doOrdinaryRotInterpolation()
    {
        LerpRotation(rot);
    }

    void LerpRotation(Vector3 rotAngle)
    {
        myTransform.rotation = Quaternion.Lerp(myTransform.rotation, Quaternion.Euler(rotAngle), rotLerpRate * Time.deltaTime);
    }

    void LerpPosition()
    {
        if (!isLocalPlayer)
        {
            if (useHistoricalPosLerping)
            {
                doHistoricalPosInterpolation();
            }
            else
            {
                doOrdinaryPosInterpolation();
            }
        }
    }

    void doHistoricalPosInterpolation()
    {
        if (syncPosList.Count > 0)
        {
            myTransform.position = Vector3.Lerp(myTransform.position, syncPosList[0], Time.deltaTime * posLerpRate);

            if (Vector3.Distance(myTransform.position, syncPosList[0]) < wayPointCloseEnoughDist)
            {
                syncPosList.RemoveAt(0);
            }

            if (syncPosList.Count > 10)
            {
                posLerpRate = fastPosLerpRate;
            }
            else
            {
                posLerpRate = normalPosLerpRate;
            }

            //Debug.Log(syncPosList.Count.ToString());
        }
    }

    void doOrdinaryPosInterpolation()
    {
        myTransform.position = Vector3.Lerp(myTransform.position, pos, Time.deltaTime * posLerpRate);
    }

    [Command]
    void CmdProvidePositionToServer(Vector3 newPos)
    {
        pos = newPos;
        //Debug.Log("Command called");
    }

    [Command]
    void CmdProvideRotationToServer(Vector3 rotation)
    {
        rot = rotation;
    }

    [ClientCallback]
    void TransmitPosition()
    {
        if (isLocalPlayer && Vector3.Distance(myTransform.position, lastPos) > transmissionDistThreshold)
        {
            CmdProvidePositionToServer(myTransform.position);
            lastPos = myTransform.position;
        }
    }

    [ClientCallback]
    void TransmitRotation()
    {
        if (isLocalPlayer)
        {
            if (CheckIfBeyondThreshold(myTransform.localEulerAngles.x, lastRot.x) ||
                CheckIfBeyondThreshold(myTransform.localEulerAngles.y, lastRot.y) ||
                CheckIfBeyondThreshold(myTransform.localEulerAngles.z, lastRot.z))
            {
                lastRot = myTransform.localEulerAngles;
                CmdProvideRotationToServer(lastRot);
            }
        }
    }

    [Client]
    void SyncRotationValues(Vector3 latestRot)
    {
        rot = latestRot;
        syncRotList.Add(rot);
    }

    [Client]
    void SyncPositionValues(Vector3 latestPos)
    {
        pos = latestPos;
        syncPosList.Add(pos);
    }

    bool CheckIfBeyondThreshold(float rot1, float rot2)
    {
        if (Mathf.Abs(rot1 - rot2) > transmissionRotThreshold)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
}