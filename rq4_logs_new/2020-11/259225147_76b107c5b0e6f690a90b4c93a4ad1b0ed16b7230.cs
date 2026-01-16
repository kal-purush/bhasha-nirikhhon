using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class StatusUp : MonoBehaviour
{
    [SerializeField] Status status;

    [SerializeField] float plusSpeed = 10;
    [SerializeField] int plusSpeedReachCount = 10;

    [SerializeField] int plusHP = 5000;
    [SerializeField] int plusHpReachCount = 10;

    [SerializeField] int plusSTR = 100;
    [SerializeField] int plusSTRReachCount = 10;

    public float GetPlusSpeed { get; private set; }
    public int GetPlusHP { get; private set; }
    public int GetPlusSTR { get; private set; }

    float speed;
    int hp;
    int str;

    // Start is called before the first frame update
    void Start()
    {
        speed = plusSpeed / plusSpeedReachCount;
        hp = plusHP / plusHpReachCount;
        str = plusSTR / plusSTRReachCount;
    }

    private void OnCollisionEnter(Collision collision)
    {
        var obj = collision.gameObject;
        var effectId = obj.GetComponent<EffectObjectID>();

        if (effectId == null||IsLimit()) return;

        var effectColor = NameDefinition.GetEffectColor(effectId.effectObjectType);

        switch (effectColor)
        {
            case NameDefinition.EffectColor.Red:
                status.SetPlusStatus(Status.Name.STR, str);
                break;
            case NameDefinition.EffectColor.Blue:
                status.SetPlusSpeed(speed);
                break;
            case NameDefinition.EffectColor.Green:
                status.SetPlusStatus(Status.Name.HP, hp);
                break;

            case NameDefinition.EffectColor.Nothing:break;
            default: break;
        }

    }

    //全てが限界値を超えたらtrue
    bool IsLimit()
    {
        return speed > plusSpeed && hp > plusHP && str > plusSTR;
    }
}