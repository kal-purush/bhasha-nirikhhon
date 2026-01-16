using UnityEngine;
using System.Collections;

[ExecuteInEditMode]
public class JointAnimatorTyrese : MonoBehaviour {

    public TyreseJoints mabones;

    [Range(-360, 360)]
    public float LeftWingANGLE;

    [Range(-360, 360)]
    public float RightWingANGLE;

    [Range(-360, 360)]
    public float TorsoANGLE;

    [Range(-360, 360)]
    public float HeadANGLE;

    [Range(-360, 360)]
    public float LeftLegANGLE;

    [Range(-360, 360)]
    public float RightLegANGLE;

    [Range(-360, 360)]
    public float LeftFootANGLE;

    [Range(-360, 360)]
    public float RightFootANGLE;

    void Update()
    {
        mabones.LeftWing.transform.localRotation = Quaternion.Euler(0, 0, LeftWingANGLE);
        mabones.RightWing.transform.localRotation = Quaternion.Euler(0, 0, RightWingANGLE);
        mabones.Torso.transform.localRotation = Quaternion.Euler(0, 0, TorsoANGLE);
        mabones.Head.transform.localRotation = Quaternion.Euler(0, 0, HeadANGLE);
        mabones.LeftLeg.transform.localRotation = Quaternion.Euler(0, 0, LeftLegANGLE);
        mabones.RightLeg.transform.localRotation = Quaternion.Euler(0, 0, RightLegANGLE);
        mabones.LeftFoot.transform.localRotation = Quaternion.Euler(0, 0, LeftFootANGLE);
        mabones.RightFoot.transform.localRotation = Quaternion.Euler(0, 0, RightFootANGLE);
    }
}

[System.Serializable]
public class TyreseJoints
{
    public GameObject LeftWing;

    public GameObject RightWing;

    public GameObject Torso;

    public GameObject Head;

    public GameObject LeftLeg;

    public GameObject RightLeg;

    public GameObject LeftFoot;

    public GameObject RightFoot;
}