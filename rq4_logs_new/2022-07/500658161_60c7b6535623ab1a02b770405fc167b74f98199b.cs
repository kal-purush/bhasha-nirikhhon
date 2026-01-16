using UnityEngine;
using System.Collections;
using System;

public class Skill1_002 : MonoBehaviour
{
    private string activeBy;
    [SerializeField]
    private NetworkIdentity networkIdentity;


    public string ActiveBy { get => activeBy; set => activeBy = value; }


    private void OnCollisionEnter2D(Collision2D collision)
    {

        var niActive = NetworkClient.serverObjects[activeBy];
        NetworkIdentity ni = collision?.gameObject?.GetComponent<NetworkIdentity>();

        // cham nhau
        if (ni.tag == "BulletThrough")
        {
            return;
        }


        // ko phai cham chinh minh


        // client trung dan gui request
        if (ni.IsControlling())
        {
            networkIdentity.GetSocket().Emit("touchSkill", new JSONObject(JsonUtility.ToJson(new TouchData()
            {
                id = networkIdentity.GetId(),
                num = 1,
                typeId = "002",
                enemyId = ni.GetId(),
                typeEnemy = "Player",

            })));


            return;
        }
        //  ai trung dan , firer gui request

        if (niActive.IsControlling() && ni.GetComponent<AiManager>() != null)
        {
            networkIdentity.GetSocket().Emit("touchSkill", new JSONObject(JsonUtility.ToJson(new TouchData()
            {
                id = networkIdentity.GetId(),
                num = 1,
                typeId = "002",
                enemyId = ni.GetId(),
                typeEnemy = "AI",
            })));
            return;
        }
    }


}